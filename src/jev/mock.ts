/**
 * Mock Judge implementation for deterministic offline testing and CI.
 *
 * Implements JudgePort without making outbound network requests.
 * Note: Per IMPLEMENTATION_HANDOFF.md, MockJudge cannot substitute for live model quality acceptance.
 */

import type { Id, Json, Basis, QuestionKey, Judgment, Probe, JudgePort } from '../contracts/interfaces.js';
import { QUESTION_SET_ID, JEV_MODEL_VERSION } from './questions.js';

export interface MockJudgeOptions {
  model?: string;
  defaultProbabilities?: Partial<Record<QuestionKey, number>>;
  evaluateFn?: (basis: Basis, state: Json) => Partial<Record<QuestionKey, number>>;
  chooseProbeFn?: (basis: Basis, state: Json, candidates: readonly Probe[]) => Id | 'none';
  probeChoice?: Id | 'none';
}

const DEFAULT_PROBABILITIES: Record<QuestionKey, number> = {
  meaningful_progress: 0.5,
  unresolved_blocker: 0.1,
  needs_host_decision: 0.1,
  repeating_without_new_information: 0.05,
  claim_conflicts_with_evidence: 0.05,
  context_sufficient: 0.95
};

export class MockJudge implements JudgePort {
  readonly model: string;
  private probabilities: Record<QuestionKey, number>;
  private probeChoice: Id | 'none';
  private evaluateFn?: (basis: Basis, state: Json) => Partial<Record<QuestionKey, number>>;
  private chooseProbeFn?: (basis: Basis, state: Json, candidates: readonly Probe[]) => Id | 'none';

  public evaluateCallCount = 0;
  public chooseProbeCallCount = 0;
  public lastEvaluatedBasis?: Basis;
  public lastEvaluatedState?: Json;

  constructor(options: MockJudgeOptions = {}) {
    this.model = options.model || JEV_MODEL_VERSION;
    this.probabilities = { ...DEFAULT_PROBABILITIES, ...options.defaultProbabilities };
    this.probeChoice = options.probeChoice ?? 'none';
    this.evaluateFn = options.evaluateFn;
    this.chooseProbeFn = options.chooseProbeFn;
  }

  setProbabilities(probabilities: Partial<Record<QuestionKey, number>>): void {
    this.probabilities = { ...this.probabilities, ...probabilities };
  }

  setProbeChoice(choice: Id | 'none'): void {
    this.probeChoice = choice;
  }

  async evaluate(basis: Basis, state: Json, signal?: AbortSignal): Promise<Judgment> {
    if (signal?.aborted) {
      throw signal.reason || new Error('Aborted');
    }

    this.evaluateCallCount++;
    this.lastEvaluatedBasis = basis;
    this.lastEvaluatedState = state;

    let dynamicProbabilities: Partial<Record<QuestionKey, number>> = {};
    if (this.evaluateFn) {
      dynamicProbabilities = this.evaluateFn(basis, state);
    }

    const merged = { ...this.probabilities, ...dynamicProbabilities };

    return {
      basis,
      model: this.model,
      questionSet: QUESTION_SET_ID,
      probabilities: merged,
      receivedAt: new Date().toISOString(),
      inputTokens: 256,
      discarded: false
    };
  }

  async chooseProbe(basis: Basis, state: Json, candidates: readonly Probe[], signal?: AbortSignal): Promise<Id | 'none'> {
    if (signal?.aborted) {
      throw signal.reason || new Error('Aborted');
    }

    this.chooseProbeCallCount++;

    if (this.chooseProbeFn) {
      return this.chooseProbeFn(basis, state, candidates);
    }

    if (this.probeChoice !== 'none') {
      const exists = candidates.some(c => c.probeId === this.probeChoice);
      if (exists) {
        return this.probeChoice;
      }
    }

    return 'none';
  }
}
