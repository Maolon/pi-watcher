/**
 * Production Jev HTTP client implementing JudgePort.
 *
 * Jev is a System 1 BERT-derived discriminator, NOT an autoregressive LLM.
 * Calls official POST /v1/systemone with fixed model jev-1.13.0.
 */

import type { Id, Json, Basis, QuestionKey, Judgment, Probe, JudgePort } from '../contracts/interfaces.js';
import {
  type JevClientOptions,
  type JevSystemOneRequest,
  type JevSystemOneResponse,
  type JevNoulAnswer,
  type JevChoiceAnswer,
  JevAuthenticationError,
  JevUnprocessableError,
  JevRateLimitError,
  JevServerError,
  JevTimeoutError,
  JevError
} from './types.js';
import { STANDARD_QUESTIONS, QUESTION_SET_ID, JEV_MODEL_VERSION, buildProbeChoiceQuestion } from './questions.js';
import { sanitizeJson } from './sanitizer.js';

const DEFAULT_BASE_URL = 'https://api.typesafe.ai';
const DEFAULT_TIMEOUT_MS = 10000;
const DEFAULT_MAX_RETRIES = 2; // Initial try + 2 retries = 3 max attempts (matches policy-defaults)

export class JevHttpClient implements JudgePort {
  private readonly apiKey: string;
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly maxRetries: number;
  private readonly fetchFn: typeof fetch;

  // Stats tracking
  private _totalRequests = 0;
  private _totalInputTokens = 0;
  private _totalOutputTokens = 0;

  constructor(options: JevClientOptions = {}) {
    const envKey = process.env.JEV_API_KEY || process.env.jev_api_key || '';
    this.apiKey = options.apiKey !== undefined ? options.apiKey : envKey;
    this.baseUrl = (options.baseUrl || process.env.JEV_BASE_URL || DEFAULT_BASE_URL).replace(/\/+$/, '');
    this.timeoutMs = options.timeoutMs ?? (process.env.JEV_TIMEOUT_MS ? parseInt(process.env.JEV_TIMEOUT_MS, 10) : DEFAULT_TIMEOUT_MS);
    this.maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
    this.fetchFn = options.fetchFn ?? globalThis.fetch;
  }

  get stats() {
    return {
      totalRequests: this._totalRequests,
      totalInputTokens: this._totalInputTokens,
      totalOutputTokens: this._totalOutputTokens
    };
  }

  /**
   * Evaluates the given state against the 6 standard questions.
   */
  async evaluate(basis: Basis, state: Json, signal?: AbortSignal): Promise<Judgment> {
    const sanitizedState = (sanitizeJson(state) || {}) as Record<string, Json>;

    const requestBody: JevSystemOneRequest = {
      model: JEV_MODEL_VERSION,
      state: sanitizedState,
      questions: STANDARD_QUESTIONS
    };

    const response = await this.postSystemOne(requestBody, signal);

    const probabilities: Partial<Record<QuestionKey, number>> = {};
    for (const key of Object.keys(STANDARD_QUESTIONS) as QuestionKey[]) {
      const answer = response.answers[key] as JevNoulAnswer | undefined;
      if (!answer || answer.type !== 'noul' || typeof answer.noul !== 'number') {
        throw new JevError(`Jev response missing valid noul probability for question '${key}'`);
      }
      // Clamped probability between 0 and 1
      probabilities[key] = Math.max(0, Math.min(1, answer.noul));
    }

    return {
      basis,
      model: response.model || JEV_MODEL_VERSION,
      questionSet: QUESTION_SET_ID,
      probabilities: probabilities as Record<QuestionKey, number>,
      receivedAt: new Date().toISOString(),
      inputTokens: response.usage?.input_tokens ?? 0,
      discarded: false
    };
  }

  /**
   * Selects the single best candidate probe to resolve an evidence gap, or 'none'.
   */
  async chooseProbe(basis: Basis, state: Json, candidates: readonly Probe[], signal?: AbortSignal): Promise<Id | 'none'> {
    if (candidates.length === 0) {
      return 'none';
    }

    const sanitizedState = (sanitizeJson(state) || {}) as Record<string, Json>;
    const choiceQuestion = buildProbeChoiceQuestion(candidates);

    const requestBody: JevSystemOneRequest = {
      model: JEV_MODEL_VERSION,
      state: sanitizedState,
      questions: {
        probe_choice: choiceQuestion
      }
    };

    const response = await this.postSystemOne(requestBody, signal);
    const answer = response.answers.probe_choice as JevChoiceAnswer | undefined;

    if (!answer || answer.type !== 'choice' || typeof answer.choice !== 'string') {
      return 'none';
    }

    const validIds = new Set(candidates.map(p => p.probeId));
    if (validIds.has(answer.choice)) {
      return answer.choice;
    }

    return 'none';
  }

  /**
   * Low-level post to /v1/systemone with retry logic.
   */
  private async postSystemOne(requestBody: JevSystemOneRequest, parentSignal?: AbortSignal): Promise<JevSystemOneResponse> {
    if (!this.apiKey) {
      throw new JevAuthenticationError(401, 'No Jev API key configured. Provide apiKey option or set JEV_API_KEY environment variable.');
    }

    const url = `${this.baseUrl}/v1/systemone`;
    let attempt = 0;

    while (attempt <= this.maxRetries) {
      attempt++;

      if (parentSignal?.aborted) {
        throw parentSignal.reason || new Error('Aborted');
      }

      const timeoutController = new AbortController();
      const timer = setTimeout(() => timeoutController.abort(), this.timeoutMs);

      const combinedAbortHandler = () => {
        timeoutController.abort();
      };
      if (parentSignal) {
        parentSignal.addEventListener('abort', combinedAbortHandler, { once: true });
      }

      try {
        const res = await this.fetchFn(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${this.apiKey}`,
            'User-Agent': 'pi-watcher/0.1.0'
          },
          body: JSON.stringify(requestBody),
          signal: timeoutController.signal
        });

        clearTimeout(timer);
        if (parentSignal) {
          parentSignal.removeEventListener('abort', combinedAbortHandler);
        }

        if (res.ok) {
          const data = (await res.json()) as JevSystemOneResponse;
          this._totalRequests++;
          if (data.usage) {
            this._totalInputTokens += data.usage.input_tokens || 0;
            this._totalOutputTokens += data.usage.output_tokens || 0;
          }
          return data;
        }

        // Non-OK responses
        const status = res.status;
        const errText = await res.text().catch(() => '');

        // 401 / 403: auth failure -> do NOT retry
        if (status === 401 || status === 403) {
          throw new JevAuthenticationError(status, errText);
        }

        // 422: unprocessable request -> do NOT retry
        if (status === 422) {
          let parsed: unknown = errText;
          try {
            parsed = JSON.parse(errText);
          } catch {
            // Keep text
          }
          throw new JevUnprocessableError(status, errText, parsed);
        }

        // 429: rate limit
        if (status === 429) {
          const retryAfterSec = parseInt(res.headers.get('Retry-After') || '', 10);
          const retryDelayMs = !isNaN(retryAfterSec) && retryAfterSec > 0
            ? retryAfterSec * 1000
            : this.calculateBackoff(attempt);

          if (attempt <= this.maxRetries) {
            await this.sleep(retryDelayMs, parentSignal);
            continue;
          }
          throw new JevRateLimitError(errText, retryDelayMs);
        }

        // 5xx / 529: transient server errors -> retry with jitter
        if (status >= 500) {
          if (attempt <= this.maxRetries) {
            const backoffMs = this.calculateBackoff(attempt);
            await this.sleep(backoffMs, parentSignal);
            continue;
          }
          throw new JevServerError(status, errText);
        }

        // Other client errors (400, 404, etc.) -> no retry
        throw new JevError(`Jev request failed with status ${status}: ${errText}`);

      } catch (err: unknown) {
        clearTimeout(timer);
        if (parentSignal) {
          parentSignal.removeEventListener('abort', combinedAbortHandler);
        }

        // Rethrow if deliberate abort or non-retryable
        // Any JevError raised from an HTTP response above has already made its retry
        // decision (429/5xx `continue` while attempts remain); 400/404 and other client
        // errors must not be retried. Only network/abort failures fall through to retry.
        if (err instanceof JevError) {
          throw err;
        }
        if (parentSignal?.aborted) {
          throw parentSignal.reason || new Error('Aborted');
        }

        // Timeout checking
        if (timeoutController.signal.aborted && !parentSignal?.aborted) {
          if (attempt <= this.maxRetries) {
            const backoffMs = this.calculateBackoff(attempt);
            await this.sleep(backoffMs, parentSignal);
            continue;
          }
          throw new JevTimeoutError(this.timeoutMs);
        }

        // Network / fetch errors
        if (attempt <= this.maxRetries) {
          const backoffMs = this.calculateBackoff(attempt);
          await this.sleep(backoffMs, parentSignal);
          continue;
        }

        if (err instanceof JevError) {
          throw err;
        }
        throw new JevError(`Jev network failure: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    throw new JevError('Max retries exceeded');
  }

  private calculateBackoff(attempt: number): number {
    // Exponential backoff with random jitter: 200ms * 2^(attempt - 1) + [0..100]ms
    const base = 200 * Math.pow(2, attempt - 1);
    const jitter = Math.floor(Math.random() * 100);
    return base + jitter;
  }

  private sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        return reject(signal.reason || new Error('Aborted'));
      }
      const t = setTimeout(resolve, ms);
      signal?.addEventListener('abort', () => {
        clearTimeout(t);
        reject(signal.reason || new Error('Aborted'));
      }, { once: true });
    });
  }
}
