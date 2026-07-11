import { applyClaudeCodeHeaders, resolveClaudeCodeIdentity } from "@cortexkit/anthropic-auth-core";
import type { Api, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import { calculateCost, createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import { buildAnthropicRequest, fromClaudeCodeToolName } from "./convert.js";

export const DEFAULT_MAX_RETRY_DELAY_MS = 60_000;

// The auth core only negotiates the 1M-context and adaptive-effort betas when
// the request has the full Claude Code agent shape (context_management +
// diagnostics + output_config + thinking + tools + system). This extension
// does not send that full shape, so request them explicitly for the models
// that need them, keyed off our own model metadata.
function selectExtraBetas(model: Model<Api>, body: any): string[] {
  const betas: string[] = [];
  if ((model?.contextWindow ?? 0) > 200_000) {
    betas.push("context-1m-2025-08-07");
  }
  if (body?.output_config?.effort) {
    betas.push("effort-2025-11-24");
  }
  return betas;
}

function mapStopReason(
  reason: string,
  stopDetails?: unknown,
): {
  stopReason: "stop" | "length" | "toolUse" | "error";
  errorMessage?: string;
} {
  switch (reason) {
    case "end_turn":
    case "pause_turn":
    case "stop_sequence":
      return { stopReason: "stop" };
    case "max_tokens":
      return { stopReason: "length" };
    case "tool_use":
      return { stopReason: "toolUse" };
    case "refusal":
      return {
        stopReason: "error",
        errorMessage:
          isObjectRecord(stopDetails) && typeof stopDetails.explanation === "string"
            ? stopDetails.explanation
            : "The model refused to complete the request",
      };
    case "sensitive":
      return {
        stopReason: "error",
        errorMessage: "Anthropic flagged the response as sensitive",
      };
    default:
      throw new Error(`Unhandled Anthropic stop reason: ${reason}`);
  }
}

function removeParserScratch(output: any): void {
  for (const block of output.content) {
    delete block.index;
    delete block.partialJson;
  }
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function requireContentBlockIndex(value: unknown): number {
  if (!Number.isInteger(value) || (value as number) < 0) {
    throw new Error("Invalid Anthropic content-block index");
  }
  return value as number;
}

const KNOWN_CONTENT_BLOCK_DELTA_TYPES = new Set([
  "text_delta",
  "thinking_delta",
  "signature_delta",
  "input_json_delta",
]);

function createOutput(model: any): any {
  return {
    role: "assistant" as const,
    content: [] as any[],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "stop" as string,
    timestamp: Date.now(),
  };
}

function updateUsage(model: any, output: any, usage: any): void {
  if (!usage) return;
  output.usage.input = usage.input_tokens ?? output.usage.input;
  output.usage.output = usage.output_tokens ?? output.usage.output;
  output.usage.cacheRead = usage.cache_read_input_tokens ?? output.usage.cacheRead;
  output.usage.cacheWrite = usage.cache_creation_input_tokens ?? output.usage.cacheWrite;
  output.usage.totalTokens =
    output.usage.input + output.usage.output + output.usage.cacheRead + output.usage.cacheWrite;
  calculateCost(model, output.usage);
}

function splitSseFrame(buffer: string): { frame: string; rest: string } | null {
  const match = /\r?\n\r?\n/.exec(buffer);
  if (!match || match.index === undefined) return null;
  return {
    frame: buffer.slice(0, match.index),
    rest: buffer.slice(match.index + match[0].length),
  };
}

async function* parseSseFrame(frame: string): AsyncGenerator<any> {
  const dataLines: string[] = [];
  for (const line of frame.split(/\r?\n/)) {
    if (!line.startsWith("data:")) continue;
    dataLines.push(line.slice(5).trim());
  }
  const data = dataLines.join("\n").trim();
  if (!data || data === "[DONE]") return;
  try {
    yield JSON.parse(data);
  } catch {
    throw new Error("Malformed Anthropic SSE JSON frame");
  }
}

export async function* parseSse(response: Response, signal?: AbortSignal): AsyncGenerator<any> {
  if (!response.body) return;
  const reader = (response.body as any).getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let reachedEof = false;
  try {
    while (true) {
      const { done, value } = await withAbort<{
        done: boolean;
        value?: Uint8Array;
      }>(reader.read(), signal);
      if (done) {
        reachedEof = true;
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      let parsed = splitSseFrame(buffer);
      while (parsed) {
        buffer = parsed.rest;
        for await (const event of parseSseFrame(parsed.frame)) yield event;
        parsed = splitSseFrame(buffer);
      }
    }
    buffer += decoder.decode();
    if (buffer.trim()) {
      for await (const event of parseSseFrame(buffer)) yield event;
    }
  } finally {
    if (!reachedEof || signal?.aborted) {
      await reader.cancel(signal?.reason).catch(() => undefined);
    }
    reader.releaseLock?.();
  }
}

type PreparedAnthropicRequest = {
  url: URL;
  headers: Headers;
  bodyText: string;
};

function mergeCustomHeaders(defaults: Headers, custom: unknown): Headers {
  const merged = new Headers(defaults);
  if (custom === undefined) return merged;
  if (!custom || typeof custom !== "object" || Array.isArray(custom)) {
    throw new TypeError("Anthropic custom headers must be an object");
  }
  for (const [name, value] of Object.entries(custom)) {
    if (value === null) merged.delete(name);
    else if (typeof value === "string") merged.set(name, value);
    else {
      throw new TypeError(`Anthropic custom header ${name} must be a string or null`);
    }
  }
  return merged;
}

async function prepareAnthropicRequest({
  model,
  context,
  streamOptions,
  accessToken,
  stripThinkingSignatures = false,
}: {
  model: Model<Api>;
  context: Context;
  streamOptions: SimpleStreamOptions;
  accessToken: string;
  stripThinkingSignatures?: boolean;
}): Promise<PreparedAnthropicRequest> {
  const identity = await withAbort(
    Promise.resolve().then(() => resolveClaudeCodeIdentity(accessToken, model.id)),
    streamOptions.signal,
  );
  const { body, bodyText } = await buildAnthropicRequest(
    model,
    context,
    streamOptions,
    identity,
    stripThinkingSignatures,
    async (payload) => {
      if (!streamOptions?.onPayload) return payload;
      const transformed = await withAbort(
        Promise.resolve(streamOptions.onPayload(payload, model)),
        streamOptions.signal,
      );
      return transformed === undefined ? payload : transformed;
    },
  );
  const defaults = applyClaudeCodeHeaders(new Headers(), accessToken, {
    body,
    identity,
    extraBetas: selectExtraBetas(model, body),
  });
  return {
    url: new URL("/v1/messages?beta=true", model.baseUrl),
    headers: mergeCustomHeaders(defaults, streamOptions?.headers),
    bodyText,
  };
}

async function sendAnthropicRequest(
  prepared: PreparedAnthropicRequest,
  model: Model<Api>,
  streamOptions: SimpleStreamOptions,
): Promise<Response> {
  if (streamOptions?.signal?.aborted) {
    throw streamOptions.signal.reason ?? new Error("Aborted");
  }
  const request = new Request(prepared.url, {
    method: "POST",
    headers: prepared.headers,
    body: prepared.bodyText,
    ...(streamOptions.signal ? { signal: streamOptions.signal } : {}),
  });
  let response: Response;
  try {
    response = await fetch(request);
  } catch (error) {
    if (streamOptions?.signal?.aborted || isAbortError(error)) throw error;
    if (error instanceof TypeError) throw new AnthropicTransportError(error);
    throw error;
  }
  if (streamOptions?.onResponse) {
    try {
      await withAbort(
        Promise.resolve(
          streamOptions.onResponse(
            {
              status: response.status,
              headers: Object.fromEntries(response.headers.entries()),
            },
            model,
          ),
        ),
        streamOptions.signal,
      );
    } catch (error) {
      await response.body?.cancel(error).catch(() => undefined);
      throw error;
    }
  }
  return response;
}

export async function withAbort<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return await promise;
  return await new Promise<T>((resolve, reject) => {
    let settled = false;
    const cleanup = () => signal.removeEventListener("abort", abort);
    const abort = () => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(signal.reason ?? new Error("Aborted"));
    };
    promise.then(
      (value) => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve(value);
      },
      (error) => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(error);
      },
    );
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
}

async function readAnthropicError(response: Response, signal?: AbortSignal): Promise<string> {
  const text = await withAbort(response.text(), signal);
  return `Anthropic request failed: HTTP ${response.status} ${text}`;
}

export function retryAfterMs(response: Response, nowMs = Date.now()): number | undefined {
  const value = response.headers.get("retry-after");
  if (!value) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const dateMs = Date.parse(value);
  if (Number.isFinite(dateMs)) return Math.max(0, dateMs - nowMs);
  return undefined;
}

export function retryDelayDecision({
  serverDelayMs,
  fallbackDelayMs,
  maxRetryDelayMs,
}: {
  serverDelayMs?: number | undefined;
  fallbackDelayMs: number;
  maxRetryDelayMs: number;
}): { delayMs: number; exceedsCap: boolean } {
  if (serverDelayMs !== undefined) {
    return {
      delayMs: serverDelayMs,
      exceedsCap: maxRetryDelayMs > 0 && serverDelayMs > maxRetryDelayMs,
    };
  }
  return {
    delayMs: maxRetryDelayMs > 0 ? Math.min(fallbackDelayMs, maxRetryDelayMs) : fallbackDelayMs,
    exceedsCap: false,
  };
}

export function isRetryableStatus(status: number): boolean {
  return status === 429 || status === 529 || (status >= 500 && status <= 599);
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

class NonRetryableAnthropicError extends Error {}

class AnthropicTransportError extends Error {
  constructor(cause: TypeError) {
    super(cause.message);
    this.name = "AnthropicTransportError";
  }
}

class AnthropicTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`Anthropic request timed out after ${timeoutMs}ms`);
    this.name = "TimeoutError";
  }
}

type OperationCancellation = {
  signal: AbortSignal | undefined;
  abortSource: () => "caller" | "timeout" | undefined;
  cleanup: () => void;
};

export function createOperationCancellation(
  callerSignal: unknown,
  timeoutValue: unknown,
): OperationCancellation {
  if (
    callerSignal !== undefined &&
    (!callerSignal ||
      typeof callerSignal !== "object" ||
      typeof (callerSignal as AbortSignal).aborted !== "boolean" ||
      typeof (callerSignal as AbortSignal).addEventListener !== "function" ||
      typeof (callerSignal as AbortSignal).removeEventListener !== "function")
  ) {
    throw new TypeError("Anthropic signal must be an AbortSignal");
  }
  const signal = callerSignal as AbortSignal | undefined;
  if (signal?.aborted) {
    return {
      signal,
      abortSource: () => "caller",
      cleanup: () => undefined,
    };
  }
  if (
    timeoutValue !== undefined &&
    (typeof timeoutValue !== "number" ||
      !Number.isFinite(timeoutValue) ||
      !Number.isInteger(timeoutValue) ||
      timeoutValue < 0)
  ) {
    throw new TypeError(`Invalid timeoutMs: ${String(timeoutValue)}`);
  }
  if (timeoutValue === undefined) {
    return {
      signal,
      abortSource: () => (signal?.aborted ? "caller" : undefined),
      cleanup: () => undefined,
    };
  }

  const timeoutMs = timeoutValue as number;
  const controller = new AbortController();
  let abortSource: "caller" | "timeout" | undefined;
  const abort = (source: "caller" | "timeout", reason: unknown) => {
    if (controller.signal.aborted) return;
    abortSource = source;
    controller.abort(reason);
  };
  const onCallerAbort = () => abort("caller", signal?.reason ?? new Error("Aborted"));
  if (signal) signal.addEventListener("abort", onCallerAbort, { once: true });
  const timer = setTimeout(() => abort("timeout", new AnthropicTimeoutError(timeoutMs)), timeoutMs);
  timer.unref?.();
  return {
    signal: controller.signal,
    abortSource: () => abortSource,
    cleanup: () => {
      clearTimeout(timer);
      if (signal) signal.removeEventListener("abort", onCallerAbort);
    },
  };
}

function isRetryableFetchError(error: unknown): boolean {
  return error instanceof AnthropicTransportError;
}

async function delay(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) throw signal.reason ?? new Error("Aborted");
  if (ms <= 0) return;
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      if (signal) signal.removeEventListener("abort", abort);
      resolve();
    }, ms);
    const abort = () => {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener("abort", abort);
      reject(signal?.reason ?? new Error("Aborted"));
    };
    if (signal) {
      if (signal.aborted) abort();
      else signal.addEventListener("abort", abort, { once: true });
    }
  });
}

async function requestWithRetries(args: {
  model: Model<Api>;
  context: Context;
  streamOptions: SimpleStreamOptions;
  accessToken: string;
  stripThinkingSignatures?: boolean;
}): Promise<{ response: Response; errorText?: string }> {
  const maxRetries = Math.max(0, args.streamOptions?.maxRetries ?? 2);
  const maxRetryDelayMs = Math.max(
    0,
    args.streamOptions?.maxRetryDelayMs ?? DEFAULT_MAX_RETRY_DELAY_MS,
  );
  const prepared = await prepareAnthropicRequest(args);
  let lastError: unknown;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      if (args.streamOptions?.signal?.aborted)
        throw args.streamOptions.signal.reason ?? new Error("Aborted");
      const response = await sendAnthropicRequest(prepared, args.model, args.streamOptions);
      if (response.ok) return { response };
      const errorText = await readAnthropicError(response, args.streamOptions?.signal);
      if (response.status === 401 || response.status === 403) {
        throw new NonRetryableAnthropicError(
          `${errorText}\nAnthropic OAuth token was rejected; run /login anthropic to refresh credentials.`,
        );
      }
      if (!isRetryableStatus(response.status) || attempt === maxRetries)
        return { response, errorText };
      const serverDelayMs = retryAfterMs(response);
      const retryDelay = retryDelayDecision({
        serverDelayMs,
        fallbackDelayMs: 250 * 2 ** attempt,
        maxRetryDelayMs,
      });
      if (retryDelay.exceedsCap) {
        throw new NonRetryableAnthropicError(
          `Anthropic requested a retry delay of ${serverDelayMs}ms, which exceeds maxRetryDelayMs=${maxRetryDelayMs}.`,
        );
      }
      await delay(retryDelay.delayMs, args.streamOptions?.signal);
    } catch (error) {
      lastError = error;
      if (
        args.streamOptions?.signal?.aborted ||
        isAbortError(error) ||
        !isRetryableFetchError(error) ||
        attempt === maxRetries
      )
        throw error;
      const retryDelay = retryDelayDecision({
        fallbackDelayMs: 250 * 2 ** attempt,
        maxRetryDelayMs,
      });
      await delay(retryDelay.delayMs, args.streamOptions?.signal);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

async function openAnthropicResponse(args: {
  model: Model<Api>;
  context: Context;
  streamOptions: SimpleStreamOptions;
  accessToken: string;
}): Promise<Response> {
  const first = await requestWithRetries(args);
  if (!first.response.ok) {
    const error =
      first.errorText ?? (await readAnthropicError(first.response, args.streamOptions?.signal));
    if (/Invalid `signature` in `thinking` block/.test(error)) {
      const retry = await requestWithRetries({
        ...args,
        stripThinkingSignatures: true,
      });
      if (retry.response.ok) return retry.response;
      throw new Error(
        retry.errorText ?? (await readAnthropicError(retry.response, args.streamOptions?.signal)),
      );
    }
    throw new Error(error);
  }
  return first.response;
}

export function streamAnthropicAuth(
  model: Model<Api>,
  context: Context,
  options: SimpleStreamOptions = {},
) {
  const stream = createAssistantMessageEventStream();
  void (async () => {
    const output = createOutput(model);
    stream.push({ type: "start", partial: output });
    let cancellation: OperationCancellation | undefined;
    try {
      cancellation = createOperationCancellation(options?.signal, options?.timeoutMs);
      const streamOptions: SimpleStreamOptions = cancellation.signal
        ? { ...options, signal: cancellation.signal }
        : options;
      if (cancellation.signal?.aborted) {
        throw cancellation.signal.reason ?? new Error("Aborted");
      }
      const accessToken = options?.apiKey ?? "";
      if (!accessToken) throw new Error("Missing Anthropic OAuth access token");
      const response = await openAnthropicResponse({
        model,
        context,
        streamOptions,
        accessToken,
      });
      const blocks = output.content;
      const openBlockIndexes = new Set<number>();
      const closedBlockIndexes = new Set<number>();
      for await (const event of parseSse(response, cancellation.signal)) {
        if (event.type === "message_start") updateUsage(model, output, event.message?.usage);
        else if (event.type === "content_block_start") {
          const blockIndex = requireContentBlockIndex(event.index);
          if (openBlockIndexes.has(blockIndex) || closedBlockIndexes.has(blockIndex)) {
            throw new Error("Malformed Anthropic content-block lifecycle");
          }
          const block = event.content_block;
          if (!isObjectRecord(block) || typeof block.type !== "string") {
            throw new Error("Unsupported Anthropic content-block type");
          }
          if (block.type === "text") {
            if (typeof block.text !== "string") {
              throw new Error("Invalid Anthropic text block");
            }
            output.content.push({
              type: "text",
              text: block.text,
              index: blockIndex,
            });
            stream.push({
              type: "text_start",
              contentIndex: output.content.length - 1,
              partial: output,
            });
          } else if (block.type === "thinking") {
            if (typeof block.thinking !== "string") {
              throw new Error("Invalid Anthropic thinking block");
            }
            output.content.push({
              type: "thinking",
              thinking: block.thinking,
              thinkingSignature: "",
              index: blockIndex,
            });
            stream.push({
              type: "thinking_start",
              contentIndex: output.content.length - 1,
              partial: output,
            });
          } else if (block.type === "redacted_thinking") {
            if (typeof block.data !== "string") {
              throw new Error("Invalid Anthropic redacted thinking block");
            }
            output.content.push({
              type: "thinking",
              thinking: "[Reasoning redacted]",
              thinkingSignature: block.data,
              redacted: true,
              index: blockIndex,
            });
            stream.push({
              type: "thinking_start",
              contentIndex: output.content.length - 1,
              partial: output,
            });
          } else if (block.type === "tool_use") {
            if (
              typeof block.id !== "string" ||
              typeof block.name !== "string" ||
              !isObjectRecord(block.input)
            ) {
              throw new Error("Invalid Anthropic tool input JSON");
            }
            output.content.push({
              type: "toolCall",
              id: block.id,
              name: fromClaudeCodeToolName(block.name, context.tools),
              arguments: { ...block.input },
              partialJson: "",
              index: blockIndex,
            });
            stream.push({
              type: "toolcall_start",
              contentIndex: output.content.length - 1,
              partial: output,
            });
          } else {
            throw new Error("Unsupported Anthropic content-block type");
          }
          openBlockIndexes.add(blockIndex);
        } else if (event.type === "content_block_delta") {
          const blockIndex = requireContentBlockIndex(event.index);
          if (!openBlockIndexes.has(blockIndex)) {
            throw new Error("Malformed Anthropic content-block lifecycle");
          }
          if (!isObjectRecord(event.delta) || typeof event.delta.type !== "string") {
            throw new Error("Malformed Anthropic content-block delta");
          }
          const contentIndex = blocks.findIndex((block: any) => block.index === blockIndex);
          const block = blocks[contentIndex];
          if (!block) {
            throw new Error("Malformed Anthropic content-block lifecycle");
          }
          if (block.redacted === true && KNOWN_CONTENT_BLOCK_DELTA_TYPES.has(event.delta.type)) {
            throw new Error("Malformed Anthropic content-block delta");
          }
          if (event.delta.type === "text_delta") {
            if (block.type !== "text" || typeof event.delta.text !== "string") {
              throw new Error("Malformed Anthropic content-block delta");
            }
            block.text += event.delta.text;
            stream.push({
              type: "text_delta",
              contentIndex,
              delta: event.delta.text,
              partial: output,
            });
          } else if (event.delta.type === "thinking_delta") {
            if (block.type !== "thinking" || typeof event.delta.thinking !== "string") {
              throw new Error("Malformed Anthropic content-block delta");
            }
            block.thinking += event.delta.thinking;
            stream.push({
              type: "thinking_delta",
              contentIndex,
              delta: event.delta.thinking,
              partial: output,
            });
          } else if (event.delta.type === "signature_delta") {
            if (block.type !== "thinking" || typeof event.delta.signature !== "string") {
              throw new Error("Malformed Anthropic content-block delta");
            }
            block.thinkingSignature = `${block.thinkingSignature ?? ""}${event.delta.signature}`;
          } else if (event.delta.type === "input_json_delta") {
            if (block.type !== "toolCall" || typeof event.delta.partial_json !== "string") {
              throw new Error("Malformed Anthropic content-block delta");
            }
            const delta = event.delta.partial_json;
            block.partialJson = `${block.partialJson ?? ""}${delta}`;
            try {
              block.arguments = JSON.parse(block.partialJson);
            } catch {
              // Keep partialJson until the next stream delta completes the JSON.
            }
            stream.push({
              type: "toolcall_delta",
              contentIndex,
              delta,
              partial: output,
            });
          }
        } else if (event.type === "content_block_stop") {
          const blockIndex = requireContentBlockIndex(event.index);
          if (!openBlockIndexes.delete(blockIndex)) {
            throw new Error("Malformed Anthropic content-block lifecycle");
          }
          closedBlockIndexes.add(blockIndex);
          const contentIndex = blocks.findIndex((block: any) => block.index === blockIndex);
          const block = blocks[contentIndex];
          if (!block) {
            throw new Error("Malformed Anthropic content-block lifecycle");
          }
          if (block.type === "toolCall" && block.partialJson.trim()) {
            let argumentsValue: unknown;
            try {
              argumentsValue = JSON.parse(block.partialJson);
            } catch {
              throw new Error("Invalid Anthropic tool input JSON");
            }
            if (!isObjectRecord(argumentsValue)) {
              throw new Error("Invalid Anthropic tool input JSON");
            }
            block.arguments = argumentsValue;
          }
          delete block.index;
          delete block.partialJson;
          if (block.type === "text") {
            stream.push({
              type: "text_end",
              contentIndex,
              content: block.text,
              partial: output,
            });
          } else if (block.type === "thinking") {
            stream.push({
              type: "thinking_end",
              contentIndex,
              content: block.thinking,
              partial: output,
            });
          } else {
            stream.push({
              type: "toolcall_end",
              contentIndex,
              toolCall: block,
              partial: output,
            });
          }
        } else if (event.type === "message_delta") {
          updateUsage(model, output, event.usage);
          // Intermediate message_delta frames may omit stop_reason; only
          // map a real value so a clean turn is not marked as an error.
          if (event.delta?.stop_reason) {
            const mapped = mapStopReason(event.delta.stop_reason, event.delta.stop_details);
            output.stopReason = mapped.stopReason;
            if (mapped.errorMessage) output.errorMessage = mapped.errorMessage;
          }
        } else if (event.type === "message_stop") {
          if (cancellation.signal?.aborted) {
            throw cancellation.signal.reason ?? new Error("Aborted");
          }
          if (openBlockIndexes.size > 0) {
            throw new Error("Anthropic stream ended with an open content block");
          }
          if (output.stopReason === "error") {
            throw new Error(output.errorMessage ?? "Anthropic stopped with an error");
          }
          removeParserScratch(output);
          stream.push({
            type: "done",
            reason: output.stopReason,
            message: output,
          });
          stream.end();
          return;
        } else if (event.type === "error")
          throw new Error(
            `Anthropic stream error: ${event.error?.message ?? JSON.stringify(event.error)}`,
          );
      }
      throw new Error("Anthropic stream ended before message_stop");
    } catch (error) {
      removeParserScratch(output);
      const stopReason = cancellation?.abortSource() === "caller" ? "aborted" : "error";
      const message = {
        ...output,
        stopReason,
        errorMessage: error instanceof Error ? error.message : String(error),
      };
      stream.push({ type: "error", reason: stopReason, error: message });
      stream.end();
    } finally {
      cancellation?.cleanup();
    }
  })();
  return stream;
}
