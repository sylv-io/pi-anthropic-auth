import assert from "node:assert/strict";
import { test } from "node:test";
import type { Context } from "@earendil-works/pi-ai";

import {
  createOperationCancellation,
  DEFAULT_MAX_RETRY_DELAY_MS,
  isRetryableStatus,
  parseSse,
  retryAfterMs,
  retryDelayDecision,
  streamAnthropicAuth,
  withAbort,
} from "../src/stream.ts";

const MODEL = {
  id: "claude-haiku-4-5",
  name: "Claude Haiku 4.5",
  provider: "anthropic",
  api: "anthropic-auth-messages",
  baseUrl: "https://api.anthropic.com",
  reasoning: true,
  input: ["text", "image"] as ("text" | "image")[],
  contextWindow: 200_000,
  maxTokens: 64_000,
  cost: { input: 1, output: 5, cacheRead: 0.1, cacheWrite: 1.25 },
};

const CONTEXT: Context = {
  systemPrompt: "be helpful",
  messages: [{ role: "user", content: "hello", timestamp: 0 }],
  tools: [],
};

function sseResponse(frames: string, init: ResponseInit = {}) {
  return new Response(frames, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
    ...init,
  });
}

async function collectEvents(stream: AsyncIterable<any>, timeoutMs = 1_000) {
  return await Promise.race([
    (async () => {
      const events: any[] = [];
      for await (const event of stream) events.push(event);
      return events;
    })(),
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error("stream did not end")), timeoutMs),
    ),
  ]);
}

function lastEvent(events: any[]) {
  return events[events.length - 1];
}

function assertSingleTerminal(events: any[], expected: "done" | "error") {
  const terminal = events.filter((event) => event.type === "done" || event.type === "error");
  assert.equal(terminal.length, 1, JSON.stringify(events));
  assert.equal(terminal[0]?.type, expected, JSON.stringify(events));
}

function assertNoParserScratch(message: any) {
  for (const block of message.content) {
    assert.equal("index" in block, false, JSON.stringify(message));
    assert.equal("partialJson" in block, false, JSON.stringify(message));
  }
}

function messageStopSse(separator = "\n\n") {
  return (
    [
      `data: ${JSON.stringify({ type: "message_start", message: { usage: { input_tokens: 1, output_tokens: 0 } } })}`,
      `data: ${JSON.stringify({ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } })}`,
      `data: ${JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "OK" } })}`,
      `data: ${JSON.stringify({ type: "content_block_stop", index: 0 })}`,
      `data: ${JSON.stringify({ type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 1 } })}`,
      `data: ${JSON.stringify({ type: "message_stop" })}`,
    ].join(separator) + separator
  );
}

function eventSse(events: unknown[]): string {
  return [...events.map((event) => `data: ${JSON.stringify(event)}`), ""].join("\n\n");
}

async function collectProtocolEvents(protocolEvents: unknown[]) {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => sseResponse(eventSse(protocolEvents));
  try {
    return await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
      }),
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
}

test("parseSse accepts CRLF frame boundaries", async () => {
  const events: string[] = [];
  for await (const event of parseSse(sseResponse(messageStopSse("\r\n\r\n"))))
    events.push(event.type);
  assert.deepEqual(events, [
    "message_start",
    "content_block_start",
    "content_block_delta",
    "content_block_stop",
    "message_delta",
    "message_stop",
  ]);
});

test("parseSse handles chunk-split frames and trailing final frame", async () => {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode('data: {"type":"message_start"}\r'));
      controller.enqueue(encoder.encode('\n\r\ndata: {"type":"message_stop"}'));
      controller.close();
    },
  });
  const events: string[] = [];
  for await (const event of parseSse(new Response(stream))) events.push(event.type);
  assert.deepEqual(events, ["message_start", "message_stop"]);
});

test("parseSse combines multiline data and rejects malformed JSON generically", async () => {
  const validBody = ['data: {"type":', 'data: "message_stop"}', ""].join("\n");
  const events: string[] = [];
  for await (const event of parseSse(sseResponse(validBody))) events.push(event.type);
  assert.deepEqual(events, ["message_stop"]);

  const secret = "do-not-leak-frame-content";
  await assert.rejects(
    async () => {
      for await (const _event of parseSse(sseResponse(`data: {not-json:${secret}}\n\n`))) {
        // The malformed frame must fail before yielding an event.
      }
    },
    (error: Error) => {
      assert.match(error.message, /Malformed Anthropic SSE JSON frame/);
      assert.doesNotMatch(error.message, new RegExp(secret));
      return true;
    },
  );
});

test("streamAnthropicAuth ends after message_stop without another request", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return sseResponse(messageStopSse());
  };
  try {
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
    });
    const events = await collectEvents(stream);
    assertSingleTerminal(events, "done");
    assert.equal(events.find((event) => event.type === "text_delta")?.delta, "OK");
    assert.equal(calls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("malformed streaming frame errors once, redacts data, and cancels an open body", async () => {
  const originalFetch = globalThis.fetch;
  const encoder = new TextEncoder();
  const secret = "stream-secret-must-not-leak";
  let cancelled = false;
  globalThis.fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(encoder.encode(`data: {bad-json:${secret}}\n\n`));
        },
        cancel() {
          cancelled = true;
        },
      }),
      { status: 200 },
    );
  try {
    const events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
      }),
    );
    assertSingleTerminal(events, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /Malformed Anthropic SSE JSON frame/);
    assert.doesNotMatch(lastEvent(events)?.error.errorMessage, new RegExp(secret));
    assert.equal(cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("closed EOF without message_stop errors once without cancelling the closed body", async () => {
  const originalFetch = globalThis.fetch;
  const encoder = new TextEncoder();
  let cancelled = false;
  globalThis.fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(
            encoder.encode(
              [
                'data: {"type":"message_start"}',
                'data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}',
                'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}',
                'data: {"type":"content_block_stop","index":0}',
                "",
              ].join("\n\n"),
            ),
          );
          controller.close();
        },
        cancel() {
          cancelled = true;
        },
      }),
      { status: 200 },
    );
  try {
    const events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
      }),
    );
    assertSingleTerminal(events, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /before message_stop/);
    assert.equal(cancelled, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("message_stop rejects every open content-block type", async () => {
  const originalFetch = globalThis.fetch;
  try {
    for (const contentBlock of [
      { type: "text", text: "" },
      { type: "thinking", thinking: "" },
      { type: "tool_use", id: "tool-1", name: "test_tool", input: {} },
    ]) {
      globalThis.fetch = async () =>
        sseResponse(
          [
            'data: {"type":"message_start"}',
            `data: ${JSON.stringify({ type: "content_block_start", index: 0, content_block: contentBlock })}`,
            'data: {"type":"message_stop"}',
            "",
          ].join("\n\n"),
        );
      const events = await collectEvents(
        streamAnthropicAuth(MODEL, CONTEXT, {
          apiKey: "test-token",
        }),
      );
      assertSingleTerminal(events, "error");
      assert.match(lastEvent(events)?.error.errorMessage, /open content block/);
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("tool input must finish as a JSON object before toolcall_end", async () => {
  const originalFetch = globalThis.fetch;
  try {
    for (const partialJson of ["{", "[]", "null", '"value"']) {
      globalThis.fetch = async () =>
        sseResponse(
          [
            'data: {"type":"message_start"}',
            'data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool-1","name":"test_tool","input":{}}}',
            `data: ${JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: partialJson } })}`,
            'data: {"type":"content_block_stop","index":0}',
            'data: {"type":"message_stop"}',
            "",
          ].join("\n\n"),
        );
      const events = await collectEvents(
        streamAnthropicAuth(MODEL, CONTEXT, {
          apiKey: "test-token",
        }),
      );
      assertSingleTerminal(events, "error");
      assert.equal(
        events.some((event) => event.type === "toolcall_end"),
        false,
      );
      assert.match(lastEvent(events)?.error.errorMessage, /Invalid Anthropic tool input JSON/);
      assertNoParserScratch(lastEvent(events)?.error);
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("parameterless tool preserves initial empty input and completes", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () =>
    sseResponse(
      [
        'data: {"type":"message_start"}',
        'data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool-1","name":"test_tool","input":{}}}',
        'data: {"type":"content_block_stop","index":0}',
        'data: {"type":"message_delta","delta":{"stop_reason":"tool_use"}}',
        'data: {"type":"message_stop"}',
        "",
      ].join("\n\n"),
    );
  try {
    const events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
      }),
    );
    assertSingleTerminal(events, "done");
    const toolEnd = events.find((event) => event.type === "toolcall_end");
    assert.deepEqual(toolEnd?.toolCall.arguments, {});
    assertNoParserScratch(lastEvent(events)?.message);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("populated tool input removes parser scratch from emitted messages", async () => {
  const events = await collectProtocolEvents([
    { type: "message_start" },
    {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "tool_use",
        id: "tool-1",
        name: "test_tool",
        input: {},
      },
    },
    {
      type: "content_block_delta",
      index: 0,
      delta: { type: "input_json_delta", partial_json: '{"path":' },
    },
    {
      type: "content_block_delta",
      index: 0,
      delta: { type: "input_json_delta", partial_json: '"file.txt"}' },
    },
    { type: "content_block_stop", index: 0 },
    { type: "message_delta", delta: { stop_reason: "tool_use" } },
    { type: "message_stop" },
  ]);

  assertSingleTerminal(events, "done");
  const toolEnd = events.find((event) => event.type === "toolcall_end");
  assert.ok(toolEnd);
  assert.deepEqual(toolEnd.toolCall.arguments, { path: "file.txt" });
  assertNoParserScratch({ content: [toolEnd.toolCall] });
  assert.deepEqual(lastEvent(events)?.message.content[0]?.arguments, {
    path: "file.txt",
  });
  assertNoParserScratch(lastEvent(events)?.message);
});

test("content-block starts reject missing, unknown, and invalid fields", async () => {
  for (const contentBlock of [
    {},
    { type: "text" },
    { type: "text", text: 1 },
    { type: "thinking" },
    { type: "thinking", thinking: 1 },
    { type: "future_block" },
    { type: "redacted_thinking" },
    { type: "redacted_thinking", data: 42 },
  ]) {
    const events = await collectProtocolEvents([
      { type: "message_start" },
      { type: "content_block_start", index: 0, content_block: contentBlock },
      { type: "message_stop" },
    ]);
    assertSingleTerminal(events, "error");
    assert.equal(
      events.some((event) => event.type === "done"),
      false,
    );
  }
});

test("text and thinking starts preserve valid nonempty initial content", async () => {
  const events = await collectProtocolEvents([
    { type: "message_start" },
    {
      type: "content_block_start",
      index: 0,
      content_block: { type: "text", text: "initial text" },
    },
    {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: " plus delta" },
    },
    { type: "content_block_stop", index: 0 },
    {
      type: "content_block_start",
      index: 1,
      content_block: { type: "thinking", thinking: "initial thought" },
    },
    {
      type: "content_block_delta",
      index: 1,
      delta: { type: "thinking_delta", thinking: " plus delta" },
    },
    { type: "content_block_stop", index: 1 },
    { type: "message_stop" },
  ]);
  assertSingleTerminal(events, "done");
  assert.equal(lastEvent(events)?.message.content[0]?.text, "initial text plus delta");
  assert.equal(lastEvent(events)?.message.content[1]?.thinking, "initial thought plus delta");
  assertNoParserScratch(lastEvent(events)?.message);
});

test("error stop reasons terminate with valid error events", async () => {
  for (const stopCase of [
    {
      reason: "refusal",
      stopDetails: { explanation: "Request refused safely" },
      expected: /Request refused safely/,
    },
    { reason: "sensitive", expected: /flagged.*sensitive/i },
    { reason: "future_reason", expected: /Unhandled Anthropic stop reason/ },
  ]) {
    const events = await collectProtocolEvents([
      { type: "message_start" },
      {
        type: "message_delta",
        delta: {
          stop_reason: stopCase.reason,
          stop_details: stopCase.stopDetails,
        },
      },
      { type: "message_stop" },
    ]);
    assertSingleTerminal(events, "error");
    assert.equal(lastEvent(events)?.reason, "error");
    assert.equal(lastEvent(events)?.error.stopReason, "error");
    assert.match(lastEvent(events)?.error.errorMessage, stopCase.expected);
    assert.equal(
      events.some((event) => event.type === "done"),
      false,
    );
  }
});

test("content-block indexes enforce strict open and closed lifecycle", async () => {
  const textStart = (index: unknown) => ({
    type: "content_block_start",
    index,
    content_block: { type: "text", text: "" },
  });
  const cases: unknown[][] = [
    [textStart(undefined)],
    [textStart(-1)],
    [textStart(0.5)],
    [textStart("0")],
    [textStart(0), textStart(0)],
    [textStart(0), { type: "content_block_stop", index: 0 }, textStart(0)],
    [
      {
        type: "content_block_delta",
        delta: { type: "text_delta", text: "missing index" },
      },
    ],
    [
      {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: "early" },
      },
    ],
    [
      textStart(0),
      { type: "content_block_stop", index: 0 },
      {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: "late" },
      },
    ],
    [{ type: "content_block_stop" }],
    [{ type: "content_block_stop", index: 0 }],
    [
      textStart(0),
      { type: "content_block_stop", index: 0 },
      { type: "content_block_stop", index: 0 },
    ],
    [
      textStart(0),
      {
        type: "content_block_delta",
        index: 1.5,
        delta: { type: "text_delta", text: "bad index" },
      },
    ],
    [textStart(0), { type: "content_block_stop", index: "0" }],
  ];
  for (const protocolCase of cases) {
    const events = await collectProtocolEvents([
      { type: "message_start" },
      ...protocolCase,
      { type: "message_stop" },
    ]);
    assertSingleTerminal(events, "error");
    assert.equal(
      events.some((event) => event.type === "done"),
      false,
    );
  }
});

test("known deltas reject mismatched blocks", async () => {
  const cases = [
    {
      block: { type: "thinking", thinking: "" },
      delta: { type: "text_delta", text: "wrong" },
    },
    {
      block: { type: "text", text: "" },
      delta: { type: "thinking_delta", thinking: "wrong" },
    },
    {
      block: { type: "text", text: "" },
      delta: { type: "signature_delta", signature: "wrong" },
    },
    {
      block: { type: "text", text: "" },
      delta: { type: "input_json_delta", partial_json: "{}" },
    },
  ];
  for (const protocolCase of cases) {
    const events = await collectProtocolEvents([
      { type: "message_start" },
      {
        type: "content_block_start",
        index: 0,
        content_block: protocolCase.block,
      },
      { type: "content_block_delta", index: 0, delta: protocolCase.delta },
      { type: "content_block_stop", index: 0 },
      { type: "message_stop" },
    ]);
    assertSingleTerminal(events, "error");
  }
});

test("known deltas require string fields and a typed delta object", async () => {
  const cases = [
    {
      block: { type: "text", text: "" },
      delta: { type: "text_delta" },
    },
    {
      block: { type: "text", text: "" },
      delta: { type: "text_delta", text: 1 },
    },
    {
      block: { type: "thinking", thinking: "" },
      delta: { type: "thinking_delta" },
    },
    {
      block: { type: "thinking", thinking: "" },
      delta: { type: "signature_delta", signature: null },
    },
    {
      block: { type: "tool_use", id: "tool-1", name: "test_tool", input: {} },
      delta: { type: "input_json_delta" },
    },
    {
      block: { type: "tool_use", id: "tool-1", name: "test_tool", input: {} },
      delta: { type: "input_json_delta", partial_json: 1 },
    },
    { block: { type: "text", text: "" }, delta: null },
    { block: { type: "text", text: "" }, delta: {} },
    { block: { type: "text", text: "" }, delta: { type: 1 } },
  ];
  for (const protocolCase of cases) {
    const events = await collectProtocolEvents([
      { type: "message_start" },
      {
        type: "content_block_start",
        index: 0,
        content_block: protocolCase.block,
      },
      { type: "content_block_delta", index: 0, delta: protocolCase.delta },
      { type: "content_block_stop", index: 0 },
      { type: "message_stop" },
    ]);
    assertSingleTerminal(events, "error");
  }
});

test("redacted thinking emits normal thinking lifecycle with opaque signature", async () => {
  const events = await collectProtocolEvents([
    { type: "message_start" },
    {
      type: "content_block_start",
      index: 0,
      content_block: { type: "redacted_thinking", data: "opaque-data" },
    },
    { type: "content_block_stop", index: 0 },
    { type: "message_stop" },
  ]);
  assertSingleTerminal(events, "done");
  assert.equal(
    events.some((event) => event.type === "thinking_start"),
    true,
  );
  assert.equal(
    events.some((event) => event.type === "thinking_end"),
    true,
  );
  assert.deepEqual(lastEvent(events)?.message.content[0], {
    type: "thinking",
    thinking: "[Reasoning redacted]",
    thinkingSignature: "opaque-data",
    redacted: true,
  });
});

test("redacted thinking rejects known thinking and signature deltas", async () => {
  for (const delta of [
    { type: "thinking_delta", thinking: "must not append" },
    { type: "signature_delta", signature: "must not append" },
  ]) {
    const events = await collectProtocolEvents([
      { type: "message_start" },
      {
        type: "content_block_start",
        index: 0,
        content_block: { type: "redacted_thinking", data: "opaque-data" },
      },
      { type: "content_block_delta", index: 0, delta },
      { type: "content_block_stop", index: 0 },
      { type: "message_stop" },
    ]);
    assertSingleTerminal(events, "error");
    assert.equal(
      events.some((event) => event.type === "thinking_end"),
      false,
    );
    assert.equal(
      events.some((event) => event.type === "done"),
      false,
    );
  }
});

test("unknown future delta is ignored only for an open known block", async () => {
  const events = await collectProtocolEvents([
    { type: "message_start" },
    {
      type: "content_block_start",
      index: 0,
      content_block: { type: "text", text: "" },
    },
    {
      type: "content_block_delta",
      index: 0,
      delta: { type: "future_delta", payload: "ignored" },
    },
    { type: "content_block_stop", index: 0 },
    { type: "message_stop" },
  ]);
  assertSingleTerminal(events, "done");
  assert.equal(
    events.some((event) => event.type === "text_delta"),
    false,
  );
  assert.equal(lastEvent(events)?.message.content[0]?.text, "");
});

test("streamAnthropicAuth retries retryable pre-stream failures", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    if (calls === 1) return new Response("temporary", { status: 500 });
    return sseResponse(messageStopSse());
  };
  try {
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 1,
      maxRetryDelayMs: 1,
    });
    const events = await collectEvents(stream);
    assert.equal(events[events.length - 1]?.type, "done");
    assert.equal(calls, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("retryAfterMs parses seconds and HTTP dates at cap boundaries", () => {
  assert.equal(retryAfterMs(new Response(null, { headers: { "retry-after": "1.5" } })), 1_500);
  const nowMs = 1_800_000_000_000;
  const equalDateDelay = retryAfterMs(
    new Response(null, {
      headers: { "retry-after": new Date(nowMs + 5_000).toUTCString() },
    }),
    nowMs,
  );
  const aboveDateDelay = retryAfterMs(
    new Response(null, {
      headers: { "retry-after": new Date(nowMs + 6_000).toUTCString() },
    }),
    nowMs,
  );
  assert.equal(equalDateDelay, 5_000);
  assert.equal(aboveDateDelay, 6_000);
  assert.equal(
    retryDelayDecision({
      serverDelayMs: equalDateDelay,
      fallbackDelayMs: 250,
      maxRetryDelayMs: 5_000,
    }).exceedsCap,
    false,
  );
  assert.equal(
    retryDelayDecision({
      serverDelayMs: aboveDateDelay,
      fallbackDelayMs: 250,
      maxRetryDelayMs: 5_000,
    }).exceedsCap,
    true,
  );
});

test("isRetryableStatus covers retry boundaries", () => {
  for (const status of [429, 529, 500, 599])
    assert.equal(isRetryableStatus(status), true, String(status));
  for (const status of [400, 401, 403, 408, 499])
    assert.equal(isRetryableStatus(status), false, String(status));
});

test("retryDelayDecision uses the Pi default and rejects only capped server delays", () => {
  assert.equal(DEFAULT_MAX_RETRY_DELAY_MS, 60_000);
  assert.deepEqual(
    retryDelayDecision({
      serverDelayMs: 100,
      fallbackDelayMs: 250,
      maxRetryDelayMs: 100,
    }),
    { delayMs: 100, exceedsCap: false },
  );
  assert.deepEqual(
    retryDelayDecision({
      serverDelayMs: 101,
      fallbackDelayMs: 250,
      maxRetryDelayMs: 100,
    }),
    { delayMs: 101, exceedsCap: true },
  );
  assert.deepEqual(
    retryDelayDecision({
      serverDelayMs: 101,
      fallbackDelayMs: 250,
      maxRetryDelayMs: 0,
    }),
    { delayMs: 101, exceedsCap: false },
  );
  assert.deepEqual(
    [250, 500, 1_000].map((fallbackDelayMs) =>
      retryDelayDecision({ fallbackDelayMs, maxRetryDelayMs: 400 }),
    ),
    [
      { delayMs: 250, exceedsCap: false },
      { delayMs: 400, exceedsCap: false },
      { delayMs: 400, exceedsCap: false },
    ],
  );
  assert.deepEqual(retryDelayDecision({ fallbackDelayMs: 250, maxRetryDelayMs: 0 }), {
    delayMs: 250,
    exceedsCap: false,
  });
});

test("streamAnthropicAuth fails immediately when Retry-After exceeds the cap", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return new Response("busy", {
      status: 429,
      headers: { "retry-after": "60" },
    });
  };
  try {
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 2,
      maxRetryDelayMs: 10,
    });
    const events = await collectEvents(stream);
    const last = events[events.length - 1];
    assert.equal(last?.type, "error");
    assert.match(last.error.errorMessage, /exceeds maxRetryDelayMs=10/);
    assert.equal(calls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("streamAnthropicAuth honors Retry-After when the cap is disabled", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    if (calls === 1) {
      return new Response("busy", {
        status: 500,
        headers: { "retry-after": "0" },
      });
    }
    return sseResponse(messageStopSse());
  };
  try {
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 1,
      maxRetryDelayMs: 0,
    });
    const events = await collectEvents(stream);
    assert.equal(events[events.length - 1]?.type, "done");
    assert.equal(calls, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("streamAnthropicAuth retries transport TypeError only", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    if (calls === 1) throw new TypeError("socket closed");
    return sseResponse(messageStopSse());
  };
  try {
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 1,
      maxRetryDelayMs: 1,
    });
    const events = await collectEvents(stream);
    assert.equal(events[events.length - 1]?.type, "done");
    assert.equal(calls, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("streamAnthropicAuth does not retry ordinary fetch or construction errors", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    throw new Error("hook failed");
  };
  try {
    let stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 2,
    });
    let events = await collectEvents(stream);
    assert.equal(events[events.length - 1]?.type, "error");
    assert.equal(calls, 1);

    stream = streamAnthropicAuth({ ...MODEL, baseUrl: "not a URL" }, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 2,
    });
    events = await collectEvents(stream);
    assert.equal(events[events.length - 1]?.type, "error");
    assert.equal(calls, 1);

    stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 2,
      signal: { aborted: false } as unknown as AbortSignal,
    });
    events = await collectEvents(stream);
    assert.equal(events[events.length - 1]?.type, "error");
    assert.equal(calls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("streamAnthropicAuth does not retry caller aborts during fetch", async () => {
  const originalFetch = globalThis.fetch;
  const controller = new AbortController();
  let calls = 0;
  globalThis.fetch = async (input: string | URL | Request) => {
    calls++;
    const signal = input instanceof Request ? input.signal : undefined;
    return await new Promise<Response>((_resolve, reject) => {
      if (signal?.aborted) {
        reject(signal.reason);
        return;
      }
      signal?.addEventListener("abort", () => reject(signal.reason), {
        once: true,
      });
    });
  };
  try {
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 2,
      signal: controller.signal,
    });
    setTimeout(() => controller.abort(new Error("cancelled by caller")), 5);
    const events = await collectEvents(stream);
    assertSingleTerminal(events, "error");
    assert.equal(lastEvent(events)?.reason, "aborted");
    assert.equal(lastEvent(events)?.error.stopReason, "aborted");
    assert.match(lastEvent(events)?.error.errorMessage, /cancelled by caller/);
    assert.equal(calls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("streamAnthropicAuth cancels retry delay without another request", async () => {
  const originalFetch = globalThis.fetch;
  const controller = new AbortController();
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return new Response("temporary", { status: 500 });
  };
  try {
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 2,
      maxRetryDelayMs: 0,
      signal: controller.signal,
    });
    setTimeout(() => controller.abort(new Error("cancelled by caller")), 5);
    const events = await collectEvents(stream);
    assertSingleTerminal(events, "error");
    assert.equal(lastEvent(events)?.reason, "aborted");
    assert.equal(lastEvent(events)?.error.stopReason, "aborted");
    assert.equal(calls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("streamAnthropicAuth honors zero retries and retry exhaustion counts", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return new Response("temporary", { status: 500 });
  };
  try {
    let stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 0,
    });
    let events = await collectEvents(stream);
    assert.equal(events[events.length - 1]?.type, "error");
    assert.equal(calls, 1);

    calls = 0;
    stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 2,
      maxRetryDelayMs: 1,
    });
    events = await collectEvents(stream);
    assert.equal(events[events.length - 1]?.type, "error");
    assert.equal(calls, 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("signature recovery receives a fresh retry budget before streaming", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    if (calls === 1)
      return new Response("Invalid `signature` in `thinking` block", {
        status: 400,
      });
    if (calls === 2) return new Response("temporary", { status: 529 });
    return sseResponse(messageStopSse());
  };
  try {
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetries: 1,
      maxRetryDelayMs: 1,
    });
    const events = await collectEvents(stream);
    assert.equal(events[events.length - 1]?.type, "done");
    assert.equal(calls, 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("streamAnthropicAuth reports rejected OAuth tokens clearly without retry", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return new Response("unauthorized", { status: 401 });
  };
  try {
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
    });
    const events = await collectEvents(stream);
    assertSingleTerminal(events, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /\/login anthropic/);
    assert.equal(calls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("onPayload transforms the final signed payload and receives the model", async () => {
  const originalFetch = globalThis.fetch;
  const payloads: any[] = [];
  const billingHeaders: string[] = [];
  globalThis.fetch = async (input: string | URL | Request) => {
    const request = input as Request;
    const payload = await request.clone().json();
    payloads.push(payload);
    const billing = payload.system?.[0]?.text;
    assert.match(billing, /x-anthropic-billing-header:.*cch=(?!00000)/);
    billingHeaders.push(billing);
    return sseResponse(messageStopSse());
  };
  try {
    let seenModel: unknown;
    let stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      onPayload(payload: any, model: unknown) {
        seenModel = model;
        payload.hook_marker = "mutated";
        return undefined;
      },
    });
    let events = await collectEvents(stream);
    assert.equal(lastEvent(events)?.type, "done", JSON.stringify(events));
    assert.equal(seenModel, MODEL);
    assert.equal(payloads[0].hook_marker, "mutated");
    assert.doesNotMatch(JSON.stringify(payloads[0]), /billing_header/);

    stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      onPayload(payload: any) {
        return { ...payload, hook_marker: "replacement" };
      },
    });
    events = await collectEvents(stream);
    assert.equal(lastEvent(events)?.type, "done", JSON.stringify(events));
    assert.equal(payloads[1].hook_marker, "replacement");
    assert.notEqual(billingHeaders[0], billingHeaders[1]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("onPayload runs once per request cycle, including signature recovery", async () => {
  const originalFetch = globalThis.fetch;
  let fetchCalls = 0;
  let payloadCalls = 0;
  globalThis.fetch = async () => {
    fetchCalls++;
    if (fetchCalls === 1) return new Response("temporary", { status: 500 });
    return sseResponse(messageStopSse());
  };
  try {
    let stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      maxRetryDelayMs: 1,
      onPayload() {
        payloadCalls++;
      },
    });
    assert.equal(lastEvent(await collectEvents(stream))?.type, "done");
    assert.equal(fetchCalls, 2);
    assert.equal(payloadCalls, 1);

    fetchCalls = 0;
    payloadCalls = 0;
    globalThis.fetch = async () => {
      fetchCalls++;
      if (fetchCalls === 1)
        return new Response("Invalid `signature` in `thinking` block", {
          status: 400,
        });
      return sseResponse(messageStopSse());
    };
    stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      onPayload() {
        payloadCalls++;
      },
    });
    assert.equal(lastEvent(await collectEvents(stream))?.type, "done");
    assert.equal(fetchCalls, 2);
    assert.equal(payloadCalls, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("onPayload failures and invalid replacements fail before fetch", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return sseResponse(messageStopSse());
  };
  try {
    for (const onPayload of [
      () => {
        throw new Error("payload hook failed");
      },
      () => ({ model: MODEL.id }),
      () => null,
    ]) {
      const events = await collectEvents(
        streamAnthropicAuth(MODEL, CONTEXT, {
          apiKey: "test-token",
          maxRetries: 3,
          onPayload,
        }),
      );
      assert.equal(lastEvent(events)?.type, "error");
    }
    assert.equal(calls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("custom headers add, override, and suppress defaults case-insensitively", async () => {
  const originalFetch = globalThis.fetch;
  let headers: Headers | undefined;
  globalThis.fetch = async (input: string | URL | Request) => {
    headers = (input as Request).headers;
    return sseResponse(messageStopSse());
  };
  try {
    const events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        headers: {
          "X-Custom-Test": "present",
          "AnThRoPiC-vErSiOn": "override",
          "ANTHROPIC-BETA": null,
          Authorization: null,
        } as any,
      }),
    );
    assert.equal(lastEvent(events)?.type, "done");
    assert.equal(headers?.get("x-custom-test"), "present");
    assert.equal(headers?.get("anthropic-version"), "override");
    assert.equal(headers?.has("anthropic-beta"), false);
    assert.equal(headers?.has("authorization"), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("malformed custom headers fail before fetch without retry", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return sseResponse(messageStopSse());
  };
  try {
    const events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        maxRetries: 3,
        headers: { "x-invalid": 42 } as any,
      }),
    );
    assert.equal(lastEvent(events)?.type, "error");
    assert.equal(calls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("onResponse runs before body reads for retry and final responses", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  let consumed = false;
  const statuses: number[] = [];
  globalThis.fetch = async () => {
    calls++;
    if (calls === 1) {
      const response = new Response("temporary", {
        status: 500,
        headers: { "x-response-test": "first" },
      });
      Object.defineProperty(response, "text", {
        value: async () => {
          consumed = true;
          return "temporary";
        },
      });
      return response;
    }
    return sseResponse(messageStopSse(), {
      headers: { "x-response-test": "final" },
    });
  };
  try {
    const events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        maxRetryDelayMs: 1,
        onResponse(response: any, model: unknown) {
          if (response.status === 500) assert.equal(consumed, false);
          assert.equal(model, MODEL);
          assert.ok(response.headers["x-response-test"]);
          statuses.push(response.status);
        },
      }),
    );
    assert.equal(lastEvent(events)?.type, "done", JSON.stringify(events));
    assert.deepEqual(statuses, [500, 200]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("onResponse failure cancels the body without retry or consumption", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  let consumed = false;
  let cancelled = false;
  globalThis.fetch = async () => {
    calls++;
    const response = new Response(
      new ReadableStream({
        cancel() {
          cancelled = true;
        },
      }),
      { status: 500 },
    );
    Object.defineProperty(response, "text", {
      value: async () => {
        consumed = true;
        return "temporary";
      },
    });
    return response;
  };
  try {
    const events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        maxRetries: 3,
        onResponse() {
          throw new Error("response hook failed");
        },
      }),
    );
    assert.equal(lastEvent(events)?.type, "error");
    assert.equal(calls, 1);
    assert.equal(consumed, false);
    assert.equal(cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("timeout aborts hanging fetch and retry delay without retry", async () => {
  const originalFetch = globalThis.fetch;
  try {
    let calls = 0;
    globalThis.fetch = async (input: string | URL | Request) => {
      calls++;
      const signal = (input as Request).signal;
      return await new Promise<Response>((_resolve, reject) => {
        if (signal.aborted) {
          reject(signal.reason);
          return;
        }
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        });
      });
    };
    let events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        timeoutMs: 50,
        maxRetries: 3,
      }),
    );
    assertSingleTerminal(events, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /timed out/i);
    assert.equal(calls, 1);

    calls = 0;
    globalThis.fetch = async () => {
      calls++;
      return new Response("busy", {
        status: 503,
        headers: { "retry-after": "1" },
      });
    };
    events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        timeoutMs: 50,
        maxRetries: 3,
      }),
    );
    assertSingleTerminal(events, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /timed out/i);
    assert.equal(calls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("timeout covers signature recovery as one logical stream call", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async (input: string | URL | Request) => {
    calls++;
    if (calls === 1)
      return new Response("Invalid `signature` in `thinking` block", {
        status: 400,
      });
    const signal = (input as Request).signal;
    return await new Promise<Response>((_resolve, reject) => {
      if (signal.aborted) {
        reject(signal.reason);
        return;
      }
      signal.addEventListener("abort", () => reject(signal.reason), {
        once: true,
      });
    });
  };
  try {
    const events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        timeoutMs: 50,
      }),
    );
    assert.equal(lastEvent(events)?.type, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /timed out/i);
    assert.equal(calls, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("timeout aborts a stalled SSE body and cancels its reader", async () => {
  const originalFetch = globalThis.fetch;
  let cancelled = false;
  globalThis.fetch = async () =>
    new Response(
      new ReadableStream({
        cancel() {
          cancelled = true;
        },
      }),
      { status: 200 },
    );
  try {
    const events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        timeoutMs: 50,
      }),
    );
    assert.equal(lastEvent(events)?.type, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /timed out/i);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(cancelled, true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("timeout cleanup removes caller listeners and preserves caller abort", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => sseResponse(messageStopSse());
  try {
    const controller = new AbortController();
    const signal = controller.signal as AbortSignal & {
      addEventListener: AbortSignal["addEventListener"];
      removeEventListener: AbortSignal["removeEventListener"];
    };
    const add = signal.addEventListener.bind(signal);
    const remove = signal.removeEventListener.bind(signal);
    let activeListeners = 0;
    signal.addEventListener = ((...args: Parameters<typeof add>) => {
      if (args[0] === "abort") activeListeners++;
      return add(...args);
    }) as typeof signal.addEventListener;
    signal.removeEventListener = ((...args: Parameters<typeof remove>) => {
      if (args[0] === "abort") activeListeners--;
      return remove(...args);
    }) as typeof signal.removeEventListener;
    let events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        signal,
        timeoutMs: 1_000,
      }),
    );
    assert.equal(lastEvent(events)?.type, "done");
    assert.equal(activeListeners, 0);

    const alreadyAborted = new AbortController();
    alreadyAborted.abort(new Error("caller stopped"));
    let fetchCalls = 0;
    globalThis.fetch = async () => {
      fetchCalls++;
      return sseResponse(messageStopSse());
    };
    events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        signal: alreadyAborted.signal,
        timeoutMs: 0,
      }),
    );
    assertSingleTerminal(events, "error");
    assert.equal(lastEvent(events)?.reason, "aborted");
    assert.equal(lastEvent(events)?.error.stopReason, "aborted");
    assert.match(lastEvent(events)?.error.errorMessage, /caller stopped/);
    assert.equal(fetchCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("withAbort settles promptly and handles late identity-like rejection", async () => {
  const controller = new AbortController();
  let rejectPending!: (error: Error) => void;
  const pending = new Promise<never>((_resolve, reject) => {
    rejectPending = reject;
  });
  const raced = withAbort(pending, controller.signal);
  controller.abort(new Error("identity bootstrap stopped"));
  await assert.rejects(raced, /identity bootstrap stopped/);
  rejectPending(new Error("late identity failure"));
  await new Promise((resolve) => setImmediate(resolve));
});

test("delayed payload and response hooks time out without late side effects", async () => {
  const originalFetch = globalThis.fetch;
  try {
    let fetchCalls = 0;
    let releasePayload!: () => void;
    const payloadGate = new Promise<void>((resolve) => {
      releasePayload = resolve;
    });
    globalThis.fetch = async () => {
      fetchCalls++;
      return sseResponse(messageStopSse());
    };
    let events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        timeoutMs: 50,
        async onPayload(payload: any) {
          await payloadGate;
          return { ...payload, late_mutation: true };
        },
      }),
    );
    assert.equal(lastEvent(events)?.type, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /timed out/i);
    assert.equal(fetchCalls, 0);
    assert.equal(
      events.filter((event) => event.type === "done" || event.type === "error").length,
      1,
    );
    releasePayload();
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(fetchCalls, 0);

    let rejectResponse!: (error: Error) => void;
    const responseGate = new Promise<never>((_resolve, reject) => {
      rejectResponse = reject;
    });
    let cancelled = false;
    globalThis.fetch = async () => {
      fetchCalls++;
      return new Response(
        new ReadableStream({
          cancel() {
            cancelled = true;
          },
        }),
        { status: 200 },
      );
    };
    events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        timeoutMs: 50,
        onResponse() {
          return responseGate;
        },
      }),
    );
    assert.equal(lastEvent(events)?.type, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /timed out/i);
    assert.equal(cancelled, true);
    assert.equal(
      events.filter((event) => event.type === "done" || event.type === "error").length,
      1,
    );
    rejectResponse(new Error("late response failure"));
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(fetchCalls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("caller abort and timeout preserve whichever reason wins first", async () => {
  const originalFetch = globalThis.fetch;
  try {
    let started!: () => void;
    globalThis.fetch = async (input: string | URL | Request) => {
      started();
      const signal = (input as Request).signal;
      return await new Promise<Response>((_resolve, reject) => {
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        });
      });
    };
    const caller = new AbortController();
    const startedPromise = new Promise<void>((resolve) => {
      started = resolve;
    });
    const stream = streamAnthropicAuth(MODEL, CONTEXT, {
      apiKey: "test-token",
      signal: caller.signal,
      timeoutMs: 500,
    });
    const collecting = collectEvents(stream);
    await startedPromise;
    caller.abort(new Error("caller won"));
    let events = await collecting;
    assertSingleTerminal(events, "error");
    assert.equal(lastEvent(events)?.reason, "aborted");
    assert.equal(lastEvent(events)?.error.stopReason, "aborted");
    assert.match(lastEvent(events)?.error.errorMessage, /caller won/);

    const lateCaller = new AbortController();
    globalThis.fetch = async (input: string | URL | Request) => {
      const signal = (input as Request).signal;
      return await new Promise<Response>((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            lateCaller.abort(new Error("caller was late"));
            reject(signal.reason);
          },
          { once: true },
        );
      });
    };
    events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        signal: lateCaller.signal,
        timeoutMs: 50,
      }),
    );
    assertSingleTerminal(events, "error");
    assert.equal(lastEvent(events)?.reason, "error");
    assert.equal(lastEvent(events)?.error.stopReason, "error");
    assert.match(lastEvent(events)?.error.errorMessage, /timed out/i);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("message_stop cancels an open SSE body while normal EOF does not", async () => {
  const originalFetch = globalThis.fetch;
  try {
    const encoder = new TextEncoder();
    let openCancelled = false;
    globalThis.fetch = async () =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(encoder.encode(messageStopSse()));
          },
          cancel() {
            openCancelled = true;
          },
        }),
        { status: 200 },
      );
    let events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
      }),
    );
    assert.equal(lastEvent(events)?.type, "done");
    assert.equal(openCancelled, true);
    assert.equal(
      events.filter((event) => event.type === "done" || event.type === "error").length,
      1,
    );

    let closedCancelled = false;
    globalThis.fetch = async () =>
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(encoder.encode(messageStopSse()));
            controller.close();
          },
          cancel() {
            closedCancelled = true;
          },
        }),
        { status: 200 },
      );
    events = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
      }),
    );
    assert.equal(lastEvent(events)?.type, "done");
    assert.equal(closedCancelled, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("invalid timeout values fail before fetch and zero times out asynchronously", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return sseResponse(messageStopSse());
  };
  try {
    const zeroCancellation = createOperationCancellation(undefined, 0);
    assert.equal(zeroCancellation.signal?.aborted, false);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(zeroCancellation.signal?.aborted, true);
    zeroCancellation.cleanup();

    for (const timeoutMs of [Number.NaN, Number.POSITIVE_INFINITY, -1, 0.5, "5"]) {
      const events = await collectEvents(
        streamAnthropicAuth(MODEL, CONTEXT, {
          apiKey: "test-token",
          timeoutMs: timeoutMs as any,
        }),
      );
      assert.equal(lastEvent(events)?.type, "error");
    }
    assert.equal(calls, 0);

    const zeroEvents = await collectEvents(
      streamAnthropicAuth(MODEL, CONTEXT, {
        apiKey: "test-token",
        timeoutMs: 0,
      }),
    );
    assert.ok(lastEvent(zeroEvents)?.type === "done" || lastEvent(zeroEvents)?.type === "error");
    if (lastEvent(zeroEvents)?.type === "error") {
      assert.match(lastEvent(zeroEvents)?.error.errorMessage, /timed out/i);
    }
    assert.equal(
      zeroEvents.filter((event) => event.type === "done" || event.type === "error").length,
      1,
    );
    assert.ok(calls <= 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("withAbort observes late rejection for an already-aborted signal", async () => {
  const controller = new AbortController();
  const callerError = new Error("caller aborted");
  controller.abort(callerError);
  let rejectLate!: (error: Error) => void;
  const late = new Promise<never>((_resolve, reject) => {
    rejectLate = reject;
  });
  const result = withAbort(late, controller.signal);
  await assert.rejects(result, callerError);
  rejectLate(new Error("late rejection"));
  await new Promise((resolve) => setImmediate(resolve));
});
