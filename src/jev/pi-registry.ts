/**
 * Jev through Pi's own model registry (Pi >= 1.1 ships TypeSafe's Jev as a classifier model).
 *
 * Credentials come from Pi, not from this package: `TYPESAFE_API_KEY`, or any provider that
 * serves Jev and is configured in Pi (`/login`, auth.json, provider env vars). The judge resolves
 * an available Jev model lazily on every pass, so a credential added mid-session via `/login`
 * is picked up without a restart.
 *
 * The watcher-q1 questions are `noul` (probability that the true criterion holds); Pi's
 * classifier API expresses the same thing as `bool` questions whose answer carries
 * `probability` = P(true). The probe choice maps to a `choice` question.
 */

import type { Id, Json, Basis, QuestionKey, Judgment, Probe, JudgePort } from '../contracts/interfaces.js';
import { STANDARD_QUESTIONS, QUESTION_SET_ID, buildProbeChoiceQuestion } from './questions.js';
import { sanitizeJson } from './sanitizer.js';

/** Structural subset of Pi's ClassifierModel. */
export interface PiClassifierModel { provider: string; id: string }

/** Structural subset of Pi's ClassifierResult. */
export interface PiClassifierResult {
  provider: string;
  model: string;
  answers: Record<string, { type: string; probability?: number; choice?: string }>;
  usage?: { input?: number };
  stopReason: 'stop' | 'error' | 'aborted';
  errorMessage?: string;
}

/** Structural subset of Pi's ModelRegistry (`ctx.modelRegistry`). */
export interface PiClassifierRegistry {
  getAvailableOfType(type: 'classifier', provider?: string): Promise<readonly PiClassifierModel[]>;
  classify(
    model: PiClassifierModel,
    context: { state: Record<string, Json>; questions: Record<string, unknown> },
    options?: { signal?: AbortSignal }
  ): Promise<PiClassifierResult>;
}

/**
 * Jev models Pi lists, in preference order: TypeSafe direct first, then gateways that serve the
 * same model. Override with PI_WATCHER_JEV_MODEL=<provider>/<modelId>.
 */
export const JEV_MODEL_PREFERENCE: ReadonlyArray<readonly [string, string]> = [
  ['typesafe', 'jev-latest'],
  ['openrouter', 'typesafe/jev-1.13'],
  ['openrouter', '~typesafe/jev-latest'],
  ['opencode', 'jev-1.13'],
  ['cloudflare-workers-ai', 'typesafe/jev'],
  ['vercel-ai-gateway', 'typesafe-ai/jev']
];

const RESOLVE_TTL_MS = 60_000;

export class PiRegistryJudge implements JudgePort {
  private cached: { at: number; model: PiClassifierModel | null } | null = null;

  constructor(
    private readonly registry: () => PiClassifierRegistry | undefined,
    private readonly override: string | undefined = process.env?.PI_WATCHER_JEV_MODEL,
    private readonly now: () => number = Date.now
  ) {}

  /** Picks the first Jev model whose provider has working credentials in Pi. */
  async resolveModel(): Promise<PiClassifierModel | null> {
    if (this.cached && this.now() - this.cached.at < RESOLVE_TTL_MS) return this.cached.model;
    const registry = this.registry();
    let model: PiClassifierModel | null = null;
    if (registry) {
      try {
        const available = await registry.getAvailableOfType('classifier');
        const wanted: ReadonlyArray<readonly [string, string]> = this.override && this.override.includes('/')
          ? [[this.override.slice(0, this.override.indexOf('/')), this.override.slice(this.override.indexOf('/') + 1)]]
          : JEV_MODEL_PREFERENCE;
        for (const [provider, id] of wanted) {
          const hit = available.find(m => m.provider === provider && m.id === id);
          if (hit) { model = hit; break; }
        }
      } catch {
        model = null;
      }
    }
    this.cached = { at: this.now(), model };
    return model;
  }

  async ready(): Promise<{ ready: boolean; reason?: string; model?: string }> {
    const model = await this.resolveModel();
    return model
      ? { ready: true, model: `${model.provider}/${model.id}` }
      : { ready: false, reason: 'no Jev credentials in Pi (set TYPESAFE_API_KEY, or add a Jev provider via /login)' };
  }

  async evaluate(basis: Basis, state: Json, signal: AbortSignal): Promise<Judgment> {
    const model = await this.requireModel();
    const questions: Record<string, unknown> = {};
    for (const [key, q] of Object.entries(STANDARD_QUESTIONS)) {
      questions[key] = { type: 'bool', instructions: q.instructions, criteria: q.criteria };
    }
    const result = await this.classify(model, state, questions, signal);
    const probabilities = {} as Record<QuestionKey, number>;
    for (const key of Object.keys(STANDARD_QUESTIONS) as QuestionKey[]) {
      const answer = result.answers[key];
      if (!answer || answer.type !== 'bool' || typeof answer.probability !== 'number' || !Number.isFinite(answer.probability)) {
        throw new Error(`Jev (${result.provider}/${result.model}) returned no valid probability for '${key}'`);
      }
      probabilities[key] = Math.max(0, Math.min(1, answer.probability));
    }
    return {
      basis,
      model: `${result.provider}/${result.model}`,
      questionSet: QUESTION_SET_ID,
      probabilities,
      receivedAt: new Date().toISOString(),
      inputTokens: result.usage?.input ?? 0,
      discarded: false
    };
  }

  async chooseProbe(_basis: Basis, state: Json, candidates: readonly Probe[], signal: AbortSignal): Promise<Id | 'none'> {
    if (candidates.length === 0) return 'none';
    const model = await this.requireModel();
    const q = buildProbeChoiceQuestion(candidates);
    const result = await this.classify(model, state, { probe_choice: { type: 'choice', instructions: q.instructions, criteria: q.criteria } }, signal);
    const choice = result.answers.probe_choice?.choice;
    return typeof choice === 'string' && candidates.some(c => c.probeId === choice) ? choice : 'none';
  }

  private async requireModel(): Promise<PiClassifierModel> {
    const model = await this.resolveModel();
    if (!model) throw new Error('Jev unavailable: no Jev credentials in Pi');
    return model;
  }

  private async classify(model: PiClassifierModel, state: Json, questions: Record<string, unknown>, signal: AbortSignal): Promise<PiClassifierResult> {
    const registry = this.registry();
    if (!registry) throw new Error('Jev unavailable: Pi model registry not attached');
    // Same redaction boundary as the direct client: nothing leaves unsanitized (I11).
    const safe = sanitizeJson(state);
    const wrapped = (safe && typeof safe === 'object' && !Array.isArray(safe) ? safe : { value: safe }) as Record<string, Json>;
    const result = await registry.classify(model, { state: wrapped, questions }, { signal });
    if (result.stopReason !== 'stop') {
      // classify() never rejects; surface its error so the engine records a failed judgment.
      // Auth failures keep their status text so the engine can disable egress (design 5.6).
      this.cached = null;
      throw new Error(`Jev (${model.provider}/${model.id}) ${result.stopReason}: ${result.errorMessage ?? 'unknown error'}`);
    }
    return result;
  }
}
