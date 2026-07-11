import assert from "node:assert/strict";
import { test } from "node:test";
import type { ExtensionAPI, ProviderConfig } from "@earendil-works/pi-coding-agent";
import anthropicAuth from "../src/index.ts";
import { streamAnthropicAuth } from "../src/stream.ts";

test("registers Pi's Anthropic catalog with the OAuth transport", () => {
  let registration: { name: string; config: ProviderConfig } | undefined;
  const pi = {
    registerProvider(name: string, config: ProviderConfig) {
      registration = { name, config };
    },
  } as ExtensionAPI;

  anthropicAuth(pi);

  assert.ok(registration);
  assert.equal(registration.name, "anthropic");
  assert.equal(registration.config.name, "Anthropic (Claude Plan OAuth)");
  assert.equal(registration.config.baseUrl, "https://api.anthropic.com");
  assert.equal(registration.config.api, "anthropic-auth-messages");
  assert.ok(registration.config.models);
  assert.ok(registration.config.models.length > 0);
  assert.ok(registration.config.models.some((model) => model.id === "claude-opus-4-6"));
  assert.ok(registration.config.models.every((model) => model.api === "anthropic-auth-messages"));
  assert.equal(registration.config.streamSimple, streamAnthropicAuth);
  assert.equal(registration.config.oauth?.name, "Anthropic Claude Plan OAuth");
});

test("OAuth API key resolution returns only the access token", () => {
  let config: ProviderConfig | undefined;
  const pi = {
    registerProvider(_name: string, providerConfig: ProviderConfig) {
      config = providerConfig;
    },
  } as ExtensionAPI;

  anthropicAuth(pi);

  assert.equal(
    config?.oauth?.getApiKey({ refresh: "refresh-secret", access: "access-secret", expires: 1 }),
    "access-secret",
  );
});
