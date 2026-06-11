# Brainstack Menu

Native macOS menu bar companion for enrolled Brainstack machines. It makes Brainstack present and observable without becoming another harness, daemon, or source of truth: all data and actions go through stable `brainctl` CLI surfaces, primarily `brainctl status --json`.

## What it shows

The menu bar icon is the status surface:

- **Green** — healthy, no degraded sections.
- **Yellow** — degraded but usable (missing optional hooks, offline curator endpoint, …).
- **Red** — broken local setup (failed config, corrupt outbox, failing section).
- **Gray** — `brainctl` missing, config missing, status timed out, or unparseable.
- Dimmed icon — showing the last good status because the latest refresh failed (stale).

The dropdown shows local sections (daemon, shared brain, outbox, hooks, skills) and control sections (brain API, curator, proposals, product updates), each with state and a one-line detail. Unknown future sections render generically by `state`/`detail`.

## Safe actions

Refresh, Open Wiki, Open Shared Brain/Config folders, Copy Redacted Diagnostics, Run Doctor, and confirmation-gated Flush Outbox, Refresh Skills, Install/Restart Daemon, Install/Repair Hooks. Every command runs off the main thread with a hard timeout.

## Operator Mode

Opt-in via Preferences. Adds curator status/run and proposal list/approve/reject/apply. Every wiki-mutating action shows a confirmation dialog with the proposal id, title, and target. The app never auto-approves or auto-applies anything, and it never stores or prints tokens. Admin actions work when `brainctl` can reach `BRAIN_ADMIN_TOKEN` locally, or when an enrolled client config has an explicit control-host SSH route so `brainctl` can forward the proposal decision to the control host. When neither path works, the app shows the decision path as blocked with command output for diagnosis.

## Build, sign, and distribute

```bash
cd apps/brainstack-menu
swift run BrainstackMenu        # development (no notifications/login item)
scripts/make-app.sh             # dist/Brainstack Menu.app + zip + dmg (Developer ID if present, else ad-hoc)
cp -R "dist/Brainstack Menu.app" /Applications/
```

Signing is automatic: the script uses `CODESIGN_IDENTITY` if set, otherwise the first
"Developer ID Application" identity in the keychain (with hardened runtime and a secure
timestamp), otherwise an ad-hoc signature for local-only use. The app icon is built into
`AppIcon.icns` from `icon/icon-1024.png` at package time.

For a distributable artifact people can download and run (signed, notarized, stapled,
Gatekeeper-accepted):

```bash
# one-time: store notary credentials
xcrun notarytool store-credentials "BrainstackNotary"

NOTARY_PROFILE=BrainstackNotary scripts/make-app.sh --notarize
# → dist/BrainstackMenu-<version>.zip + .dmg (stapled and Gatekeeper-checked)
```

The repo release script can attach it as a release asset:

```bash
BRAINSTACK_RELEASE_MENU_APP=1 NOTARY_PROFILE=BrainstackNotary scripts/release.sh
# → dist/BrainstackMenu-<version>.zip/.dmg + .sha256 files alongside the brainctl assets
```

CI can use App Store Connect API-key notarization instead of a local notary profile by
setting `APP_STORE_CONNECT_API_KEY_PATH`, `APP_STORE_CONNECT_API_KEY_ID`, and
`APP_STORE_CONNECT_API_ISSUER_ID`.

Tests:

```bash
swift test
```

## Configuration

Preferences (stored in `UserDefaults`, never secrets):

- `brainctl` path — defaults to `~/.local/bin/brainctl`, falls back to a controlled set of standard locations.
- Config path — defaults to `~/.config/brainstack/brainstack.yaml`.
- Poll interval — 15s / 30s (default) / 60s / 5m; also refreshes when the menu opens.
- Launch at login (bundled app only).
- Notifications — off by default; fire only on state transitions (broken, recovered, outbox stuck, curator failing, proposals awaiting action in Operator Mode).
- Enable Operator Mode.

## Boundaries

- Integration boundary is `brainctl`; the app never edits Brainstack files or calls braind HTTP endpoints directly.
- Brainstack correctness lives in the daemon/hooks/server; the app failing or quitting never blocks Codex, hooks, or the daemon.
- Diagnostics are redacted (token-like values, bearer headers, env secrets, invites) but may include local paths and machine names.
