/**
 * Types and error classes for Jev (System 1 BERT-based Fast Classifier).
 *
 * Notice: Jev is NOT an autoregressive generative LLM (like GPT or Claude).
 * It is a discriminative, bidirectional-encoder (BERT-style) System 1 model
 * that takes structured state and criteria-bound questions, and directly outputs
 * normalized probabilities (noul, 0.0-1.0) or discrete choices without text generation.
 */

import type { Id, Json, Basis, QuestionKey, Judgment, Probe } from '../contracts/interfaces.js';

export interface NoulCriterion {
  type: 'noul';
  instructions: string;
  criteria: {
    true: string;
    false: string;
  };
}

export interface ChoiceCriterion {
  type: 'choice';
  instructions: string;
  criteria: Record<string, string>;
  options?: Record<string, string>;
  requiredFallback?: 'none';
}

export type JevQuestion = NoulCriterion | ChoiceCriterion;

export interface JevSystemOneRequest {
  model: 'jev-1.13.0';
  state: Record<string, Json>;
  questions: Record<string, JevQuestion>;
}

export interface JevNoulAnswer {
  type: 'noul';
  noul: number;
}

export interface JevChoiceAnswer {
  type: 'choice';
  choice: string;
}

export type JevAnswer = JevNoulAnswer | JevChoiceAnswer;

export interface JevUsage {
  input_tokens: number;
  output_tokens: number;
}

export interface JevSystemOneResponse {
  model: string;
  answers: Record<string, JevAnswer>;
  usage: JevUsage;
}

export interface JevClientOptions {
  apiKey?: string;
  baseUrl?: string;
  timeoutMs?: number;
  maxRetries?: number;
  fetchFn?: typeof fetch;
}

export class JevError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'JevError';
  }
}

export class JevAuthenticationError extends JevError {
  readonly status: number;
  constructor(status: number, message: string) {
    super(`Jev authentication failed (${status}): ${message}. Check JEV_API_KEY.`);
    this.name = 'JevAuthenticationError';
    this.status = status;
  }
}

export class JevUnprocessableError extends JevError {
  readonly status: number;
  readonly details: unknown;
  constructor(status: number, message: string, details?: unknown) {
    super(`Jev unprocessable entity (${status}): ${message}`);
    this.name = 'JevUnprocessableError';
    this.status = status;
    this.details = details;
  }
}

export class JevRateLimitError extends JevError {
  readonly retryAfterMs?: number;
  constructor(message: string, retryAfterMs?: number) {
    super(`Jev rate limited: ${message}`);
    this.name = 'JevRateLimitError';
    this.retryAfterMs = retryAfterMs;
  }
}

export class JevServerError extends JevError {
  readonly status: number;
  constructor(status: number, message: string) {
    super(`Jev server error (${status}): ${message}`);
    this.name = 'JevServerError';
    this.status = status;
  }
}

export class JevTimeoutError extends JevError {
  constructor(timeoutMs: number) {
    super(`Jev request timed out after ${timeoutMs}ms`);
    this.name = 'JevTimeoutError';
  }
}
