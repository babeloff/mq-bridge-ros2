# AGENTS.md

## Project Snapshot

`mq-bridge-app` is a Rust + Svelte application vendored into the `mq-bridge`
repository as its own Cargo workspace, with its own `Cargo.lock`; the root
workspace excludes `apps`. Run npm commands from this directory. The engine it
builds on is described in the root [AGENTS.md](../../AGENTS.md).

- Backend/runtime: Rust (`crates/core`, `crates/cli`, `crates/desktop`)
- UI: Svelte 5 + Vite (`ui/src`), utilizing Runes for state management.
- Legacy migration: mostly completed; remaining compatibility shims still exist.

## Current UI Architecture

- Main app bootstrapping: `ui/src/bootstrap.ts`, `ui/src/main.ts`
- Main tabs/components:
  - `ui/src/components/PublishersPanel.svelte`
  - `ui/src/components/ConsumersPanel.svelte`
  - `ui/src/components/RoutesPanel.svelte`
  - `ui/src/components/SettingsPanel.svelte`
- State/stores:
  - `ui/src/lib/stores.ts`
  - `ui/src/lib/runtime-status.ts`
  - Modernized state handling via Svelte 5 `$state` and `$derived` runes.
- Runtime bridge helpers:
  - `ui/src/lib/runtime-window.ts`

## Important Recent Behavior Decisions

1. Consumer tab/save behavior
   - Save keeps the active consumer subtab (no definition/messages flicker).

2. Runtime status indicator
   - Topbar runtime status is sourced from polled runtime state, not stale legacy globals.

## Testing Guidance

### Which layer to write in

Pick the cheapest layer that can actually fail for the bug you have in mind, and
write the test there. Going up a layer costs roughly 100x the runtime.

| The bug is about | Layer | Where |
| --- | --- | --- |
| A pure function: grouping, parsing, a format, a schema, a URL | Vitest, node env (no pragma) | `tests/unit/*.test.ts` |
| DOM the view controllers build by hand | Vitest + `// @vitest-environment jsdom` | `tests/unit/*.test.ts` |
| A Svelte component's rendered output or its callbacks | Vitest, mounted for real | `tests/unit/component-regressions.test.ts` |
| A state machine with many orderings (select / edit / delete / add) | Vitest, model-based | `tests/unit/publishers-view-model.test.ts` |
| CSS the markup depends on | Vitest, real cascade via `getComputedStyle` | `tests/unit/style-regressions.test.ts` |
| A button doing *nothing* | Playwright, automatic | `button-sweep.spec.js` — no new test needed |
| A button doing the *wrong thing* | Playwright | `button-outcomes.spec.js` |
| A flow across views, or the app talking to the server | Playwright | `ui.spec.js` |
| Keyboard/screen-reader reachability | Playwright + axe | `a11y.spec.js` |
| Layout and spacing | Playwright screenshot | `ui-visual.spec.js` |

Two rules that decide most cases:

- **If it can be a unit test, it must be.** A Playwright test that only checks
  rendered text belongs in jsdom.
- **Do not add a Playwright test for a new button.** The sweep already clicks
  every button it can reach and fails on the ones that do nothing. Add to
  `button-outcomes.spec.js` only when the specific *outcome* matters.

### What each Playwright file owns

- `ui.spec.js` — cross-view flows and real server round trips.
- `button-sweep.spec.js` — every reachable button produces some observable
  effect. Grows by itself as buttons are added.
- `button-outcomes.spec.js` — the specific outcome of specific controls.
- `a11y.spec.js` — axe scan per view, against a shrinking known-violation list.
- `peer-status.spec.js` — the peer sidebar, with `/peer-status` stubbed.
- `settings.spec.js` — the config view.
- `ui-visual.spec.js` — screenshot baselines.
- `showcase.spec.js` — records a demo video; opt-in via `SHOWCASE=true`.
- `fixtures.js` / `helpers.js` / `app-server.js` — shared base, not tests.

### Shared rules

- Import `test`/`expect` from `tests/playwright/fixtures.js`, never from
  `@playwright/test`. That base fails a test on any uncaught exception,
  `console.error`, failed request or 5xx response — so every spec hardens every
  other spec's ground.
- Build configs with `makeConfig()` from `helpers.js` and declare only the
  publishers/consumers/routes the test asserts on. Do not paste a config shell.
- Reset state with `resetConfig(page, config)` from `helpers.js`.
- `gotoView` waits for the shell, **not** for the sidebar. Anything that counts
  list items must first wait for the count the server reports — counting
  straight after navigating is a race, and it has already caused flakes.
- Each worker runs its own app process on its own port (`app-server.js`), so
  tests never share `/config`. Never hard-code the UI port: `DISPLAY_UI_ADDR` in
  a config is display data that a screenshot pins, not somewhere to connect.

### Commands

- `npm run test:unit` — Vitest. `npm run test:unit:coverage` for a report.
- `cargo test -p mq-bridge-app-core -p mq-bridge-app` — the Rust side.
- `npm run test:ui` — chromium only locally (~2min). CI runs all three engines;
  `npm run test:ui:all` forces that locally, `PW_CHROMIUM_ONLY=1` forces chromium
  anywhere.

### Things that have bitten before

- The sweep runs one test per view *family* so the families can go on separate
  workers. Do not split it per state: dedup is per family, and a finer split
  multiplies clicks instead of dividing them. A fourth test reports the buttons
  no view reaches, which is only answerable across families.
- firefox and webkit run only the cross-browser specs listed in
  `playwright.config.js`; that is where engine-specific form-control behaviour
  has bitten.
- The visual suite skips itself on any platform with no committed baseline. To
  add the Linux ones CI needs, run the App workflow manually with
  `update_visual_baselines=true` and commit the uploaded artifact.
- Do not use LLM-based screenshot comparison.
- Do not run `npm run test:ui:update-screenshots` just to make failing tests
  pass. Update baselines only after reviewing the diff and confirming the layout
  change is intentional.

## Known Active Areas

- Further reduction of legacy/global `window` dependencies.
- Completion of the transition from legacy Svelte stores to Svelte 5 `$state` and `$derived` Runes.
- Remaining migration cleanup from `*-view.ts` controller modules into cleaner component/service boundaries.
- Enhanced real-time monitoring features and throughput visualization.
- Additional consumer traffic E2E coverage can still be expanded.

## Storage direction

Use a workspace-based storage model as the long-term target.

Workspace/domain data should still live in the workspace config where appropriate, but we no longer treat `localStorage` as UI-only state. Local browser/Tauri storage may be used for message history, traces, headers, payloads, and similar cached runtime data, but it should be encrypted at rest when the selected mode calls for it.

Threat model:

- Encrypted local storage is for offline inspection after shutdown, similar to encrypted swap or encrypted temp files.
- It is not meant to defend against malicious JavaScript, XSS, compromised frontend code, or arbitrary code execution inside the running app.

User-facing storage/security modes should stay simple even if the internal implementation uses more detailed key-provider and encryption abstractions.

CLI target modes:

- `unencrypted`: config is plain, secrets may be stored inline, and messages/local storage are plain.
- `env-secrets`: config is plain and sensitive values are extracted to env vars/placeholders. Messages/local storage are plain.
- `env-secrets-temporary-messages`: config is plain, secrets are extracted to env vars/placeholders, and messages/local storage are encrypted with a random process key. Message history is intentionally lost after restart. This is the preferred CLI default.

Tauri target modes:

- `unencrypted`: config is plain, secrets may be stored inline, and messages/local storage are plain.
- `keychain-secrets`: config is plain, sensitive values are stored in the OS key store/keychain, and messages/local storage are plain.
- `encrypted-config-temporary-messages`: config is encrypted with a persistent random key stored in the OS key store/keychain, while messages/local storage are encrypted with a separate random process key and intentionally lost after restart. This is the preferred Tauri default when a usable key store exists.
- `encrypted-config-persistent-messages`: config is encrypted with a persistent random key stored in the OS key store/keychain, and messages/local storage are encrypted with a separate persistent random key stored in the OS key store/keychain. This should be opt-in rather than the default.

Fallbacks:

- Do not assume the OS key store exists or is writable.
- When no usable OS key store is available in Tauri, fall back to modes that are honest about persistence. Temporary encrypted messages with an ephemeral process key are acceptable; fake persistence is not.
- The backend should expose storage/security status to the UI so the UI can explain whether message history is unencrypted, temporary, or persistently encrypted.

Recommended runtime status shape:

```ts
type StorageSecurityInfo = {
  encrypted: boolean;
  persistent: boolean;
  keySource: "none" | "os-key-store" | "ephemeral-process" | "env";
  configEncrypted: boolean;
  messagesEncrypted: boolean;
  messagesPersistent: boolean;
  reason?: "key-store-unavailable" | "key-store-write-failed" | "cli-mode";
};
```

Important behavior:

- If message decryption fails and the message key is ephemeral, clear the old encrypted message storage and continue with empty messages.
- If config decryption fails with a persistent key, show a recoverable error and offer reset or migration options. Do not silently delete config.

Recommended architecture:

- Keep encryption and decryption out of scattered UI call sites.
- Use a small storage/encryption abstraction with a mode enum, backend-exposed `StorageSecurityInfo`, key-provider abstraction, encryptor/decryptor abstraction, and encrypted JSON storage wrapper.
- Message history storage can be more disposable than config storage. Config persistence must be stricter and more conservative.

Encrypted storage format:

- Use an algorithm-pluggable encrypted envelope from the beginning.
- Prefer `nonce` over `iv` in the envelope naming.
- Include version, algorithm id, key id, nonce, and ciphertext.
- Bind ciphertext to its logical storage location with AAD when possible, for example `mq-bridge-app:localStorage:messages` or `mq-bridge-app:config`.

Example:

```ts
type EncryptedEnvelope = {
  v: number;
  alg: "AES-256-GCM" | "AES-256-GCM-SIV" | "XCHACHA20-POLY1305";
  kid: string;
  nonce: string;
  ciphertext: string;
};
```

Algorithm guidance:

- Avoid AES-CBC, unauthenticated AES-CTR, custom crypto constructions, and opaque \"secure localStorage\" libraries.
- If encryption happens in frontend JS, start with AES-256-GCM via WebCrypto using a fresh random 96-bit nonce for each encryption.
- If encryption happens in Rust/backend/Tauri, prefer an AEAD abstraction. AES-256-GCM-SIV is attractive if the crate support is solid; AES-256-GCM is an acceptable first step.
- Keep the envelope algorithm-pluggable either way so key rotation and future algorithm upgrades stay possible.

Saving should not blindly stop or restart routes. Compare current, saved, and applied runtime state. We should add manual triggers to restart routes or consumers.

When changing persistence code, avoid introducing ad hoc storage paths. Reuse the central workspace/config model plus the shared encrypted storage abstraction instead of scattering one-off `localStorage` logic.

## XSS

Make sure we don't print user / network input as html in UI, as we need to be protected against XSS. There also should be a basic protection against CSRF in browser mode.

## UI direction

Prefer small, boring, understandable UI code over clever abstractions.

Avoid adding new UI dependencies unless they clearly reduce complexity.

Keep the visual style minimal and unobtrusive. Prefer simple native-like controls when Web Components introduce event or rendering issues.

When changing forms, preserve keyboard usability and avoid hidden persistence side effects.

## Operational Notes

- Dev backend script: `dev/scripts/dev-backend.mjs`
- Playwright uses a temporary config file in `/tmp` for deterministic test setup.
- CLI defaults auto-enable UI/metrics when no persisted config exists.

## Quick Start

1. Install deps: `npm install`
2. Run app: `npm run dev`
3. Unit tests: `npm run test:unit`
4. UI tests: `npm run test:ui`
