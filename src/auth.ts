import { authorize, exchange, refreshClaudeOAuthToken } from "@cortexkit/anthropic-auth-core";
import type { OAuthCredentials, OAuthLoginCallbacks } from "@earendil-works/pi-ai";

export async function loginAnthropic(callbacks: OAuthLoginCallbacks): Promise<OAuthCredentials> {
  const auth = await authorize("max");
  callbacks.onAuth({ url: auth.url });

  const callback = await callbacks.onPrompt({
    message: "Paste the Claude OAuth callback URL or code:",
  });
  const result = await exchange(callback, auth.verifier, auth.redirectUri, auth.state);
  if (result.type !== "success") {
    throw new Error("Anthropic OAuth exchange failed");
  }

  return {
    refresh: result.refresh,
    access: result.access,
    expires: result.expires,
  };
}

export async function refreshAnthropicToken(
  credentials: OAuthCredentials,
): Promise<OAuthCredentials> {
  const refreshed = await refreshClaudeOAuthToken({
    refreshToken: credentials.refresh,
  });
  return {
    refresh: refreshed.refresh,
    access: refreshed.access,
    expires: refreshed.expires,
  };
}
