import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { loginAnthropic, refreshAnthropicToken } from "./auth.js";
import { ANTHROPIC_AUTH_API, getAnthropicModels } from "./models.js";
import { streamAnthropicAuth } from "./stream.js";

export default function anthropicAuth(pi: ExtensionAPI): void {
  pi.registerProvider("anthropic", {
    name: "Anthropic (Claude Plan OAuth)",
    baseUrl: "https://api.anthropic.com",
    api: ANTHROPIC_AUTH_API,
    models: getAnthropicModels(),
    oauth: {
      name: "Anthropic Claude Plan OAuth",
      login: loginAnthropic,
      refreshToken: refreshAnthropicToken,
      getApiKey: (credentials) => credentials.access,
    },
    streamSimple: streamAnthropicAuth,
  });
}
