import assert from "node:assert/strict";
import { test } from "node:test";

import { buildAnthropicRequest } from "../src/convert.ts";

const OPUS_48 = {
  id: "claude-opus-4-8",
  provider: "anthropic",
  api: "anthropic-auth-messages",
  contextWindow: 1_000_000,
  maxTokens: 128_000,
  compat: { forceAdaptiveThinking: true },
  thinkingLevelMap: { xhigh: "xhigh" },
};

const SONNET_45 = {
  id: "claude-sonnet-4-5",
  provider: "anthropic",
  api: "anthropic-auth-messages",
  contextWindow: 200_000,
  maxTokens: 64_000,
};

const HAIKU_45 = {
  id: "claude-haiku-4-5",
  provider: "anthropic",
  api: "anthropic-auth-messages",
  contextWindow: 200_000,
  maxTokens: 64_000,
};

function baseContext(overrides: Record<string, unknown> = {}) {
  return {
    systemPrompt: "be helpful",
    messages: [{ role: "user", content: "hello" }],
    tools: [],
    ...overrides,
  };
}

test("adaptive-thinking model with reasoning sends adaptive thinking + effort, no budget", async () => {
  const { body } = await buildAnthropicRequest(
    OPUS_48,
    baseContext(),
    { reasoning: "high" },
    undefined,
  );
  assert.equal(body.thinking.type, "adaptive");
  assert.equal(body.thinking.budget_tokens, undefined);
  assert.equal(body.output_config.effort, "high");
});

test("xhigh maps through thinkingLevelMap for adaptive effort", async () => {
  const { body } = await buildAnthropicRequest(
    OPUS_48,
    baseContext(),
    { reasoning: "xhigh" },
    undefined,
  );
  assert.equal(body.output_config.effort, "xhigh");
});

test("non-adaptive model with reasoning uses fixed budget thinking", async () => {
  const { body } = await buildAnthropicRequest(
    SONNET_45,
    baseContext(),
    { reasoning: "high" },
    undefined,
  );
  assert.equal(body.thinking.type, "enabled");
  assert.ok(body.thinking.budget_tokens > 0);
  assert.ok(body.thinking.budget_tokens < body.max_tokens);
  assert.equal(body.output_config, undefined);
});

test("non-adaptive thinking is omitted when max_tokens cannot fit minimum budget", async () => {
  for (const maxTokens of [1, 2, 512, 1024]) {
    const { body } = await buildAnthropicRequest(
      SONNET_45,
      baseContext(),
      { reasoning: "high", maxTokens },
      undefined,
    );
    assert.equal(body.max_tokens, maxTokens);
    assert.equal(body.thinking, undefined);
  }
});

test("non-adaptive thinking budget is strictly below small max_tokens", async () => {
  const { body } = await buildAnthropicRequest(
    SONNET_45,
    baseContext(),
    { reasoning: "high", maxTokens: 1025 },
    undefined,
  );
  assert.equal(body.max_tokens, 1025);
  assert.equal(body.thinking.type, "enabled");
  assert.equal(body.thinking.budget_tokens, 1024);
});

test("reasoning off omits the thinking block", async () => {
  const { body } = await buildAnthropicRequest(
    HAIKU_45,
    baseContext(),
    { reasoning: "off" },
    undefined,
  );
  assert.equal(body.thinking, undefined);
});

test("max_tokens is clamped to the model maximum", async () => {
  const { body } = await buildAnthropicRequest(
    SONNET_45,
    baseContext(),
    { maxTokens: 999_999 },
    undefined,
  );
  assert.equal(body.max_tokens, SONNET_45.maxTokens);
});

test("parameterless tools convert without throwing", async () => {
  const { body } = await buildAnthropicRequest(
    HAIKU_45,
    baseContext({ tools: [{ name: "ping", description: "no params" }] }),
    {},
    undefined,
  );
  assert.equal(body.tools.length, 1);
  assert.deepEqual(body.tools[0].input_schema.properties, {});
  assert.deepEqual(body.tools[0].input_schema.required, []);
});

test("signed body uses plan-compatible SDK client metadata", async () => {
  const { bodyText } = await buildAnthropicRequest(
    OPUS_48,
    baseContext(),
    { reasoning: "high" },
    undefined,
  );
  assert.ok(!bodyText.includes("cch=00000;"));
  assert.ok(bodyText.includes("cc_entrypoint=sdk-cli"));
  assert.ok(bodyText.includes("Claude Agent SDK"));
});

test("tool call is dropped when no matching tool result follows", async () => {
  const { body } = await buildAnthropicRequest(
    OPUS_48,
    baseContext({
      messages: [
        { role: "user", content: "do it" },
        {
          role: "assistant",
          api: "anthropic-auth-messages",
          provider: "anthropic",
          content: [{ type: "toolCall", id: "call_1", name: "Bash", arguments: {} }],
        },
      ],
    }),
    {},
    undefined,
  );
  const assistant = body.messages.find((m: any) => m.role === "assistant");
  assert.equal(assistant, undefined);
});
