import {
  applyClaudeCodeMetadata,
  buildBillingHeaderValue,
  CLAUDE_CODE_ENTRYPOINT,
  CLAUDE_CODE_IDENTITY,
  type ClaudeCodeIdentity,
  orderClaudeCodeBody,
  signRequestBody,
} from "@cortexkit/anthropic-auth-core";
import type { Model, SimpleStreamOptions, ThinkingLevel } from "@earendil-works/pi-ai";

type RequestModel = Pick<
  Model<"anthropic-messages">,
  "id" | "maxTokens" | "thinkingLevelMap" | "compat"
>;
type RequestOptions = Omit<SimpleStreamOptions, "reasoning"> & {
  reasoning?: ThinkingLevel | "off";
};

const CLAUDE_CODE_TOOLS = new Map(
  [
    "Read",
    "Write",
    "Edit",
    "Bash",
    "Grep",
    "Glob",
    "AskUserQuestion",
    "TodoWrite",
    "WebFetch",
    "WebSearch",
  ].map((name) => [name.toLowerCase(), name]),
);

function sanitize(text: unknown): string {
  return String(text ?? "").replace(/[\uD800-\uDFFF]/g, "\uFFFD");
}

function sanitizeToolId(id: unknown): string {
  const sanitized = String(id || "tool_call_unknown").replace(/[^a-zA-Z0-9_-]/g, "_");
  return (sanitized || "tool_call_unknown").slice(0, 256);
}

function toClaudeCodeToolName(name: string): string {
  return CLAUDE_CODE_TOOLS.get(String(name).toLowerCase()) ?? name;
}

export function fromClaudeCodeToolName(name: string, tools?: Array<{ name: string }>): string {
  const lower = String(name).toLowerCase();
  return tools?.find((tool) => tool.name.toLowerCase() === lower)?.name ?? name;
}

function convertTextAndImages(content: any): any {
  if (!Array.isArray(content)) return sanitize(content);
  if (!content.some((item: any) => item.type === "image")) {
    return content
      .filter((item: any) => item.type === "text")
      .map((item: any) => sanitize(item.text))
      .join("\n");
  }
  const blocks = content
    .map((item: any) => {
      if (item.type === "text") return { type: "text", text: sanitize(item.text) };
      if (!item.data) return null;
      return {
        type: "image",
        source: { type: "base64", media_type: item.mimeType, data: item.data },
      };
    })
    .filter(Boolean);
  if (!blocks.some((block: any) => block.type === "text"))
    blocks.unshift({ type: "text", text: "(see attached image)" });
  return blocks;
}

function convertMessages(messages: any[], stripThinkingSignatures = false): any[] {
  const result: any[] = [];
  for (let index = 0; index < messages.length; index++) {
    const message = messages[index];
    if (!message) continue;
    if (message.role === "user") {
      const content =
        typeof message.content === "string"
          ? sanitize(message.content)
          : convertTextAndImages(message.content);
      if (typeof content !== "string" || content.trim()) result.push({ role: "user", content });
    } else if (message.role === "assistant") {
      const followingToolResultIds = new Set<string>();
      for (
        let nextIndex = index + 1;
        nextIndex < messages.length && messages[nextIndex]?.role === "toolResult";
        nextIndex++
      ) {
        followingToolResultIds.add(sanitizeToolId(messages[nextIndex].toolCallId));
      }
      const blocks: any[] = [];
      const canReplayThinkingSignature =
        !stripThinkingSignatures &&
        message.api === "anthropic-auth-messages" &&
        message.provider === "anthropic";
      for (const block of message.content || []) {
        if (block.type === "text" && block.text?.trim())
          blocks.push({ type: "text", text: sanitize(block.text) });
        else if (block.type === "thinking" && block.thinking?.trim()) {
          if (canReplayThinkingSignature && block.thinkingSignature) {
            // Anthropic validates signatures against the exact thinking text it
            // emitted. Do not sanitize or otherwise normalize signed thinking,
            // and do not reuse opaque signatures from other providers.
            blocks.push({
              type: "thinking",
              thinking: String(block.thinking),
              signature: block.thinkingSignature,
            });
          } else blocks.push({ type: "text", text: sanitize(block.thinking) });
        } else if (block.type === "toolCall") {
          const id = sanitizeToolId(block.id);
          if (followingToolResultIds.has(id)) {
            blocks.push({
              type: "tool_use",
              id,
              name: toClaudeCodeToolName(block.name),
              input: block.arguments,
            });
          }
        }
      }
      if (blocks.length) result.push({ role: "assistant", content: blocks });
    } else if (message.role === "toolResult") {
      const toolResults: any[] = [];
      let nextIndex = index;
      while (nextIndex < messages.length && messages[nextIndex]?.role === "toolResult") {
        const next = messages[nextIndex];
        const converted = convertTextAndImages(next.content);
        const content = Array.isArray(converted)
          ? converted
          : [
              {
                type: "text",
                text: converted || (next.isError ? "Error" : ""),
              },
            ].filter((x: any) => x.text);
        toolResults.push({
          type: "tool_result",
          tool_use_id: sanitizeToolId(next.toolCallId),
          content,
          is_error: next.isError,
        });
        nextIndex++;
      }
      index = nextIndex - 1;
      result.push({ role: "user", content: toolResults });
    }
  }
  return result;
}

function convertTools(tools?: any[]): any[] | undefined {
  if (!tools?.length) return undefined;
  return tools.map((tool) => {
    const params = tool.parameters ?? {};
    return {
      name: toClaudeCodeToolName(tool.name),
      description: tool.description,
      input_schema: {
        type: "object",
        properties: params.properties ?? {},
        required: params.required ?? [],
      },
    };
  });
}

function addEphemeralCacheControl(body: any): void {
  const lastTool = body.tools?.[body.tools.length - 1];
  if (lastTool) lastTool.cache_control = { type: "ephemeral" };
  const lastSystem = body.system?.[body.system.length - 1];
  if (lastSystem) lastSystem.cache_control = { type: "ephemeral" };
  for (let i = body.messages.length - 1; i >= 0; i--) {
    const content = body.messages[i]?.content;
    if (body.messages[i]?.role === "user" && Array.isArray(content)) {
      const last = content[content.length - 1];
      if (last && typeof last === "object") last.cache_control = { type: "ephemeral" };
      break;
    }
  }
}

function reasoningLevel(options: RequestOptions): ThinkingLevel | undefined {
  return options.reasoning === "off" ? undefined : options.reasoning;
}

function adaptiveThinkingEffort(model: RequestModel, reasoning: ThinkingLevel): string {
  const normalized = reasoning === "minimal" ? "low" : reasoning;
  const mapped = model.thinkingLevelMap?.[normalized] ?? normalized;
  return mapped === "max" ? "xhigh" : mapped;
}

export async function buildAnthropicRequest(
  model: RequestModel,
  context: any,
  options: RequestOptions,
  identity: ClaudeCodeIdentity | undefined,
  stripThinkingSignatures = false,
  transformPayload?: (payload: unknown) => unknown | Promise<unknown>,
): Promise<{ body: any; bodyText: string }> {
  const messages = convertMessages(context.messages || [], stripThinkingSignatures);
  const system = [
    {
      type: "text",
      text: buildBillingHeaderValue(messages, undefined, CLAUDE_CODE_ENTRYPOINT),
    },
    { type: "text", text: CLAUDE_CODE_IDENTITY },
  ];
  if (context.systemPrompt?.trim())
    system.push({ type: "text", text: sanitize(context.systemPrompt) });
  const reasoning = reasoningLevel(options);
  const modelMaxTokens = model.maxTokens ?? 16_384;
  const requestedMaxTokens = options?.maxTokens ?? modelMaxTokens;
  const maxTokens = Math.min(modelMaxTokens, Math.max(requestedMaxTokens, 1));
  const body: any = {
    model: model.id,
    max_tokens: maxTokens,
    stream: true,
    system,
    messages,
  };
  const tools = convertTools(context.tools);
  if (tools?.length) body.tools = tools;
  if (reasoning && model.compat?.forceAdaptiveThinking) {
    body.thinking = { type: "adaptive" };
    body.output_config = {
      effort: adaptiveThinkingEffort(model, reasoning),
    };
  } else if (reasoning) {
    const budgets: Record<string, number> = {
      minimal: 1024,
      low: 4096,
      medium: 8192,
      high: 12000,
      xhigh: 16000,
    };
    const requestedBudget =
      options.thinkingBudgets?.[reasoning as keyof typeof options.thinkingBudgets] ??
      budgets[reasoning] ??
      8192;
    // Anthropic requires fixed thinking budgets to be strictly below
    // max_tokens. If the caller requests too few output tokens to fit the
    // minimum budget, omit thinking rather than constructing an invalid body.
    if (maxTokens > 1024) {
      const thinkingBudget = Math.min(requestedBudget, maxTokens - 1);
      body.thinking = {
        type: "enabled",
        budget_tokens: Math.max(1024, thinkingBudget),
      };
    }
  }
  addEphemeralCacheControl(body);
  if (identity) applyClaudeCodeMetadata(body, identity);
  const transformed = transformPayload ? await transformPayload(body) : undefined;
  const finalBody = transformed === undefined ? body : transformed;
  const orderedText = JSON.stringify(orderClaudeCodeBody(finalBody));
  // signRequestBody silently returns the input unchanged when the billing
  // header placeholder is missing (e.g. upstream reordered the system blocks
  // or changed the header text). Detect that drift loudly instead of sending
  // an unsigned request with a placeholder fingerprint.
  if (!orderedText.includes("cch=00000;")) {
    throw new Error(
      "anthropic-auth: missing billing-header signing placeholder; " +
        "request body shape changed and cannot be signed",
    );
  }
  const bodyText = await signRequestBody(orderedText);
  if (bodyText.includes("cch=00000;")) {
    throw new Error("anthropic-auth: request body was not signed (placeholder still present)");
  }
  return { body: finalBody, bodyText };
}
