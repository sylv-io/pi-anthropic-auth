# Contributing

Contributions are welcome. Keep changes focused and include tests for observable behavior changes.

## Set up

Requirements:

- Node.js 22 or newer
- npm

Install dependencies:

```bash
npm ci
```

## Quality checks

Run the complete local quality check:

```bash
npm run check
```

This runs Biome formatting and lint checks, TypeScript, and the automated tests. Apply safe formatting and lint fixes with:

```bash
npm run format
npm run lint:fix
```

Inspect the files that would be included in the package:

```bash
npm run pack:check
```

## Tests

Tests should protect meaningful request conversion, authentication, streaming, retry, cancellation, or error-handling behavior. Avoid tests that merely duplicate the type checker or lock in private helper structure.

Automated tests must not require live credentials or network access. Never commit OAuth credentials, Pi authentication files, callback URLs, or captured requests containing sensitive headers.

## Pull requests

Before opening a pull request:

1. Run `npm run check`.
2. Run `npm run pack:check`.
3. Review the diff for credentials, generated files, and unrelated formatting changes.
4. Update `README.md` or `CHANGELOG.md` when user-visible behavior changes.

Pull requests should explain what changed, why it changed, and any compatibility or security impact.

## Manual verification

Changes to authentication or request transport should be tested manually from a clean local installation when practical. Do not post tokens or authentication files in issues or pull requests.
