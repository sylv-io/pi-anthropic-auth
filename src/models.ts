import { getModels } from "@earendil-works/pi-ai/compat";
import type { ProviderConfig } from "@earendil-works/pi-coding-agent";

export const ANTHROPIC_AUTH_API = "anthropic-auth-messages";

export function getAnthropicModels(): NonNullable<ProviderConfig["models"]> {
  return getModels("anthropic").map((model) => ({
    id: model.id,
    name: model.name,
    api: ANTHROPIC_AUTH_API,
    reasoning: model.reasoning,
    thinkingLevelMap: model.thinkingLevelMap,
    input: model.input,
    cost: model.cost,
    contextWindow: model.contextWindow,
    maxTokens: model.maxTokens,
    compat: model.compat,
  }));
}
