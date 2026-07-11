# pi-anthropic-auth

Use an Anthropic Claude plan with the [Pi coding agent](https://github.com/earendil-works/pi) through OAuth.

This Pi extension allows you to authenticate with Anthropic through OAuth and use an eligible Claude plan instead of an Anthropic API key. Requests are sent directly from your machine to Anthropic. No relay or fallback account is involved.

While loaded, the extension replaces Pi's built-in `anthropic` provider. It retains Pi's built-in model catalog but routes those models through the extension's OAuth-aware streaming implementation.

> [!IMPORTANT]
> This is an unofficial community extension. It relies on Anthropic's current OAuth and request behavior, which may change without notice. Review Anthropic's terms and your plan eligibility before using it.

## Requirements

- Node.js 22 or newer
- Pi 0.80.6 or newer
- An Anthropic account with an eligible Claude plan

The initial release is tested against Pi 0.80.6. It follows Pi's current extension API and does not include compatibility shims for older releases.

## Install

Install the extension from GitHub:

```bash
pi install git:github.com/sylv-io/pi-anthropic-auth
```

Restart Pi, then authenticate:

```text
/login anthropic
```

Follow the browser flow and paste the callback URL or authorization code into Pi when prompted.

To try the extension without adding it to your installed packages:

```bash
pi -e git:github.com/sylv-io/pi-anthropic-auth
```

## How it works

The extension:

1. Registers OAuth login and token refresh support for the `anthropic` provider.
2. Preserves Pi's built-in Anthropic model catalog.
3. Converts Pi conversations and tools to Anthropic Messages requests.
4. Applies the Claude client identity, headers, metadata, and request signing required by the OAuth flow.
5. Streams text, reasoning, tool calls, usage, retries, cancellation, and errors back through Pi's standard event protocol.

The extension does not proxy requests through a third-party server.

## Security

Pi extensions run with the same permissions as Pi itself. Only install code you trust, and review updates before applying them.

OAuth credentials are managed by Pi's authentication storage. This extension does not intentionally log access or refresh tokens. Do not include authentication files or tokens in bug reports.

## Provider behavior

Loading this extension overrides Pi's built-in `anthropic` provider. Existing Anthropic model definitions remain available, but requests use the extension's OAuth transport. Disable or remove the extension if you want Pi's standard Anthropic API-key behavior.

## Pi extra-usage warning

Pi may display an Anthropic extra-usage warning after this extension is enabled. Pi shows this warning for any OAuth credential stored for the `anthropic` provider. It does not detect that this extension has replaced Pi's standard transport with its plan-compatible transport.

The warning alone does not indicate that this extension is using paid extra usage. It describes Pi's built-in Anthropic subscription transport and is not specific to this extension.

To hide it interactively, open `/settings` and select:

```text
Warnings → Anthropic extra usage → false
```

Alternatively, add this setting to `settings.json`:

```json
{
  "warnings": {
    "anthropicExtraUsage": false
  }
}
```

This setting only hides the warning. It does not change request routing or billing behavior. It is also global, so it suppresses the warning if you later return to Pi's standard Anthropic OAuth transport. Enable it again when appropriate.

## Reliability and stream semantics

The provider supports Pi stream options for custom headers, payload replacement,
response inspection, cancellation, request deadlines, and retry limits. Payload
replacement occurs before Claude Code-compatible identity headers are generated. The
response hook runs after headers arrive and before the body is consumed. Hook
failures are reported directly and are not retried.

Requests retry HTTP 429, 529, and other 5xx responses, plus fetch transport
`TypeError` failures. The default is two retries after the initial request.
Exponential fallback delays start at 250 milliseconds. `Retry-After` is honored
only up to the configurable delay cap, which defaults to 60 seconds.
Authentication failures, malformed requests, hook failures, caller cancellation,
and stream errors after a response begins are not replayed.

A caller-supplied timeout applies to the request and response stream. Caller
cancellation produces an `aborted` terminal, while a request timeout remains an
`error`. Cancellation-source precedence is preserved when both occur close
together.

The stream fails closed on malformed server-sent events, invalid tool JSON,
invalid content-block lifecycle transitions, open content blocks at message stop,
and premature end of file. Refusal, sensitive-content, and unknown stop reasons produce an error
terminal instead of an invalid successful terminal. Exactly one terminal event
is emitted.

Incremental tool JSON is retained only while a tool block is assembled.
Parser-only `index` and `partialJson` fields are removed before completed or
failed messages leave the provider. Completed tool arguments must parse as a
JSON object before the tool call is accepted.

## Remove

```bash
pi remove git:github.com/sylv-io/pi-anthropic-auth
```

Restart Pi after removing the package.

## Troubleshooting

### Authentication is rejected

Run `/login anthropic` again. The extension reports rejected OAuth credentials without printing the token.

### A model stops working

Anthropic may change model availability or OAuth behavior independently of this extension. Update Pi and the extension, then authenticate again. Open an issue with the Pi version, extension commit, model ID, and sanitized error message if the problem remains.

### Return to API-key authentication

Remove or disable this extension and restart Pi. The extension intentionally owns the `anthropic` provider while loaded.

## Development

```bash
npm ci
npm run check
npm run pack:check
```

Useful individual commands:

```bash
npm run format
npm run lint
npm run typecheck
npm test
```

Automated tests use mocked network responses and do not require Anthropic credentials. Before a release, maintainers should also perform a manual login and a minimal request from a clean installation.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow.

## License

[MIT](LICENSE)
