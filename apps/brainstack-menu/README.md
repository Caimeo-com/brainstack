# Brainstack Menu

Native macOS menu bar installer and companion for Brainstack machines. A signed DMG includes the app plus a bundled standalone `brainctl`; first launch can install that binary to `~/.local/bin/brainctl`, enroll the Mac from an invite, and run lifecycle repair. After setup, the app stays a lightweight status/control surface: all data and actions go through stable `brainctl` CLI surfaces, primarily fast `brainctl status --json --skip-fleet` plus a separately cached `brainctl fleet status --json --no-fetch` lane.

## First-run setup

On a fresh Mac:

1. Drag `Brainstack Menu.app` to `/Applications` and open it.
2. Click **Set Up…** when the app reports that `brainctl` is missing.
3. Paste a fresh `bs1_…` invite from the control host.

The app writes the invite to a private temporary file, runs `brainctl enroll --invite-file …`, deletes the temporary file, then runs `brainctl lifecycle repair --config …`. The invite is never stored in Preferences, diagnostics, command arguments, or action logs. The bundled helper is copied to `~/.local/bin/brainctl` first so hooks and launchd services point at a stable path, not `/Volumes`, Downloads, or the app bundle.

For an already enrolled Mac, **Set Up / Repair Brainstack…** runs the same stable-binary install followed by lifecycle repair.

## What it shows

The menu bar icon is the status surface:

- **Green** — healthy, no degraded sections.
- **Yellow** — degraded but usable (missing optional hooks, offline curator endpoint, a fleet machine behind `origin/main`, …).
- **Red** — broken local setup (failed config, corrupt outbox, failing section).
- **Gray** — `brainctl` missing, config missing, status timed out, or unparseable.
- Dimmed icon — showing the last good status because the latest refresh failed (stale).

The dropdown shows local sections (daemon, shared brain, outbox, hooks, skills), control sections (brain API, curator, proposals, product updates), and a Fleet section with each known machine, reachability, source head, dirty/behind state, and service status. Fleet is refreshed independently from the main status check so local/control health can render quickly; the app keeps the last complete fleet list when a transient partial probe returns fewer machines. If a direct control-host source probe fails but fleet already confirms the control host is reachable, clean, and current, the app treats that as a transient probe hiccup instead of a user-facing outage. Unknown future sections render generically by `state`/`detail`.

Tailscale is shown as a first-class local prerequisite when the profile depends on a tailnet control host. If Tailscale is stopped or missing, the app shows one root-cause attention row with an **Open** action and suppresses the downstream Brain API, curator, proposal, fleet, control-host, and remote-only daemon freshness warnings from the top summary until Tailscale is online.

## Safe actions

Refresh, Open Wiki, Open Shared Brain/Config folders, Copy Redacted Diagnostics, Run Doctor, and confirmation-gated Send Saved Writes, Retry Saved Writes, Refresh Skills, Install/Restart Daemon, and Install/Repair Hooks. The overflow menu includes **Import Skills…**, which scans default skill locations or a selected folder/`SKILL.md`, shows numbered candidates, and imports the selected skills into the shared brain through `brainctl skills import --select`. It also includes **Uploads…**, a drag/drop utility for copying local files to a selected Brainstack machine's private uploads folder, and **Folder Packs…**, which preflights and syncs reusable local folders to a selected machine, then lets the operator sync, attach, detach, delete, or clean unused pack copies. Uploads and Folder Packs list recent/private machine paths, copy remote paths, and delete selected items after confirmation. When Operator Mode is enabled and saved writes are paused or damaged, the outbox attention row also shows a destructive Discard action; it deletes only the local saved-write queue after confirmation and should be used only when those imports, memories, or proposals are obsolete. Fleet machine rows show an Update button only when that machine is behind; the button runs `brainctl fleet update <machine>` and confirms before pulling, rebuilding, upgrading, and restarting services. Every command runs off the main thread with a hard timeout.

## Operator Mode

Opt-in via Preferences. Adds a decision-oriented proposal queue with Review Now, Merge Suggestions, Needs Context, Conflicts/Stale, Reviewed, and All lanes. Search covers proposal meaning and provenance; sorting can prioritize review leverage, attention, oldest, or newest. Arrow-key navigation advances through the visible queue.

The decision pane leads with the rendered memory or source entry, applicability boundaries, source coverage, review checks, line impact, and destination guard. Sources, wrapped diff, and technical metadata remain available in separate tabs without dominating the default review. Structural completeness and merge-match confidence are labelled separately; neither is presented as proof that a lesson is semantically correct.

Apply calls the concrete `brainctl proposals apply` path. Every wiki-mutating action still requires confirmation, but the queue then removes the selected proposal immediately and saves the decision in the background. If the destination rejects it, the unresolved proposal returns on the next reload. Multi-select uses the checkboxes beside queue rows and supports applying or rejecting up to 20 selected items as one operator action; the confirmation lists every affected proposal, detail packets load sequentially, and decisions are serialized behind the scenes to respect the control host's write path. Proposals for the same destination must be merged or reviewed separately, and a successful apply immediately removes older open proposals that the control host supersedes for that destination. Related raw selections from one review group can also be merged into a new decision packet without including the rest of the group. **Needs work…** persists operator feedback and returns the proposal to the Needs Context lane. **Move to bottom** only defers selected items for the current review session and does not change Brainstack.

The app never auto-accepts or auto-applies anything, and it never stores or prints tokens. Admin actions work when `brainctl` can reach `BRAIN_ADMIN_TOKEN` locally, or when an enrolled client config has an explicit control-host SSH route so `brainctl` can forward the proposal decision to the control host. When neither path works, the app reports the decision failure and restores the unresolved proposal on reload.

## Build, sign, and distribute

```bash
cd apps/brainstack-menu
swift run BrainstackMenu        # development (no notifications/login item)
scripts/make-app.sh             # dist/Brainstack Menu.app + zip + dmg with bundled brainctl (Developer ID if present, else ad-hoc)
cp -R "dist/Brainstack Menu.app" /Applications/
```

Signing is automatic: the script uses `CODESIGN_IDENTITY` if set, otherwise the first
"Developer ID Application" identity in the keychain (with hardened runtime and a secure
timestamp), otherwise an ad-hoc signature for local-only use. The bundled `brainctl` is
signed as nested code before the parent app is signed. The app icon is built into
`AppIcon.icns` from `icon/icon-1024.png` at package time.

Local builds target the current Mac architecture by default. The release wrapper sets
`BRAINSTACK_MENU_ARCHES="arm64 x64"` so the published app and bundled helper are universal.
Use `BRAINSTACK_MENU_BRAINCTL_SOURCE=/path/to/brainctl` only for local smoke tests with a
known helper binary.

For a distributable artifact people can download and run (signed, notarized, stapled,
Gatekeeper-accepted):

```bash
# one-time: store notary credentials
xcrun notarytool store-credentials "BrainstackNotary"

NOTARY_PROFILE=BrainstackNotary scripts/make-app.sh --notarize
# → dist/BrainstackMenu-<version>.zip + .dmg (stapled and Gatekeeper-checked)
```

The repo release script can attach it as an optional release asset:

```bash
BRAINSTACK_RELEASE_MENU_APP=1 NOTARY_PROFILE=BrainstackNotary scripts/release.sh
# → dist/BrainstackMenu-<version>.zip/.dmg + .sha256 files alongside the brainctl assets
```

The GitHub release workflow always builds the CLI installer assets first. Tag releases
also build and publish the signed/notarized menu-app lane; manual workflow runs can opt
in with `include_menu_app`. Missing Apple signing or notarization secrets block the menu
app lane, not the standalone CLI asset build.

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
- Try to open Tailscale when Brainstack starts — off by default; when enabled, the app opens Tailscale once after launch if `brainctl status --json` reports that Tailscale is stopped.
- Notifications — off by default; fire only on state transitions (broken, recovered, outbox stuck, curator failing, proposals awaiting action in Operator Mode).
- Enable Operator Mode.

## Boundaries

- Integration boundary is `brainctl`; the app never edits Brainstack files or calls braind HTTP endpoints directly.
- Brainstack correctness lives in the daemon/hooks/server; the app failing or quitting never blocks Codex, hooks, or the daemon.
- Diagnostics are redacted (token-like values, bearer headers, env secrets, invites) but may include local paths and machine names.
