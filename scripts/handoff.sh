#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/handoff.sh [--mode review|forensic] [--out DIR]

Defaults:
  --mode review
  --out  ${TMPDIR:-/tmp}

Environment:
  BRAINSTACK_HANDOFF_UTC=YYYYMMDDTHHMMSSZ  Override bundle timestamp.

The bundle is for review/audit handoff only. It is not a release artifact.
USAGE
}

mode="review"
out_dir="${TMPDIR:-/tmp}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --mode)
      mode="${2:-}"
      shift 2
      ;;
    --out)
      out_dir="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ "$mode" != "review" ] && [ "$mode" != "forensic" ]; then
  echo "invalid mode: $mode" >&2
  exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [ -n "$(git status --porcelain)" ]; then
  echo "handoff refused: git tree is dirty" >&2
  git status --short >&2
  exit 1
fi

if ! command -v bun >/dev/null 2>&1; then
  echo "handoff refused: bun is required" >&2
  exit 1
fi
if ! command -v rg >/dev/null 2>&1; then
  echo "handoff refused: ripgrep (rg) is required for secret scanning" >&2
  exit 1
fi
if ! command -v zip >/dev/null 2>&1; then
  echo "handoff refused: zip is required" >&2
  exit 1
fi

utc="${BRAINSTACK_HANDOFF_UTC:-$(date -u +%Y%m%dT%H%M%SZ)}"
top="handoff-${utc}"
out_dir="$(cd "$out_dir" && pwd)"
bundle_dir="${out_dir}/${top}"
zip_path="${out_dir}/${top}.zip"

rm -rf "$bundle_dir" "$zip_path" "${zip_path}.sha256"
mkdir -p "$bundle_dir"/{source,generated,command-outputs,service-state,shared-brain}

product_head="$(git rev-parse HEAD)"
shared_brain_root="${SHARED_BRAIN_ROOT:-$HOME/shared-brain/staging/shared-brain}"
private_factory_root="${PRIVATE_DEV_FACTORY_ROOT:-$HOME/private-dev-factory}"

shared_head="not-present"
if [ -d "$shared_brain_root/.git" ]; then
  shared_head="$(git -C "$shared_brain_root" rev-parse HEAD)"
fi

factory_head="not-present"
if [ -d "$private_factory_root/.git" ]; then
  factory_head="$(git -C "$private_factory_root" rev-parse HEAD)"
fi

# Exactly one source representation: source/ as a git archive at HEAD.
git archive --format=tar HEAD | tar -C "$bundle_dir/source" -xf -

custom_config="$bundle_dir/generated/custom-client.yaml"
cat > "$custom_config" <<'YAML'
schema_version: 1
profile: client-macos
machine:
  name: client-laptop
  user: operator
paths:
  home: /Users/operator
client:
  localPath: /Users/operator/brain/customer-zero-shared-brain
  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git
brain:
  publicBaseUrl: https://brain-control.example.ts.net
YAML

bun run packages/brainctl/src/main.ts bootstrap-client \
  --profile client-macos \
  --config "$custom_config" \
  --out "$bundle_dir/generated/client-bootstrap-custom-path" \
  > "$bundle_dir/command-outputs/bootstrap-client-custom-path.txt" 2>&1

state_config="$bundle_dir/generated/custom-state-control.yaml"
cat > "$state_config" <<'YAML'
schema_version: 1
profile: control
machine:
  name: brain-control
  user: operator
paths:
  home: /home/operator
  stateRoot: /home/operator/.local/state/customer-brainstack
brain:
  publicBaseUrl: https://brain-control.example.ts.net
telemux:
  enabled: false
YAML

bun run packages/brainctl/src/main.ts join-worker \
  --config "$state_config" \
  --worker brain-worker \
  > "$bundle_dir/generated/join-worker-custom-state.md" 2>&1

{
  echo "product_head=$product_head"
  echo "mode=$mode"
  git status --short
} > "$bundle_dir/command-outputs/git-status.txt"

bun test > "$bundle_dir/command-outputs/bun-test.txt" 2>&1

if command -v tailscale >/dev/null 2>&1; then
  {
    tailscale status 2>&1 || true
    echo
    echo "--- whois valkyrie ---"
    tailscale whois valkyrie 2>&1 || true
    echo
    echo "--- whois erbine ---"
    tailscale whois erbine 2>&1 || true
  } > "$bundle_dir/command-outputs/tailscale-summary.txt"
fi

if curl -fsS --max-time 2 http://127.0.0.1:8080/health > "$bundle_dir/service-state/braind-health.json" 2>/dev/null; then
  :
else
  echo "braind health unavailable on http://127.0.0.1:8080/health" > "$bundle_dir/service-state/braind-health.txt"
  rm -f "$bundle_dir/service-state/braind-health.json"
fi

systemctl is-active shared-brain.service > "$bundle_dir/service-state/shared-brain-system-is-active.txt" 2>&1 || true
systemctl --user is-active telemux.service > "$bundle_dir/service-state/telemux-user-is-active.txt" 2>&1 || true

if [ "$mode" = "forensic" ]; then
  git log --oneline -20 > "$bundle_dir/command-outputs/git-log.txt"
  systemctl status shared-brain.service --no-pager > "$bundle_dir/service-state/shared-brain-system-status.txt" 2>&1 || true
  systemctl --user status telemux.service --no-pager > "$bundle_dir/service-state/telemux-user-status.txt" 2>&1 || true
  if command -v tailscale >/dev/null 2>&1; then
    tailscale debug prefs > "$bundle_dir/service-state/tailscale-prefs.txt" 2>&1 || true
  fi
fi

if [ -d "$shared_brain_root" ]; then
  for file in AGENTS.md AGENTS.shared-client.md CLAUDE.md; do
    if [ -f "$shared_brain_root/$file" ]; then
      cp "$shared_brain_root/$file" "$bundle_dir/shared-brain/$file"
    fi
  done
fi

cat > "$bundle_dir/HANDOFF.md" <<EOF
# brainstack ${mode} handoff

This is a REVIEW handoff bundle, not a release artifact.

## Scope Of This Pass

- Documented the Tailscale control/worker caveats found while bringing up valkyrie and erbine.
- Added \`scripts/handoff.sh\` so future handoff bundles use one source representation only.
- Generated this bundle with \`source/\` as the sole source representation.
- Excluded compiled binaries, dist output, dependency trees, git metadata, env files, private keys, tokens, caches, and Finder/macOS junk.

## Exact Changed Files

See \`command-outputs/git-status.txt\` for the source tree state used to build this bundle.

## Valkyrie Production Touches

- Tailscale prefs were adjusted outside the product repo: valkyrie now requests \`tag:brain\`; erbine now has \`tag:brain-worker\`; Tailscale SSH is disabled on erbine.
- No brainstack service code was restarted by this script.

## Exact Validations Run

- \`bun test\`
- \`brainctl bootstrap-client\` with a custom \`client.localPath\`
- \`brainctl join-worker\` with a custom \`paths.stateRoot\`
- Optional local \`tailscale status/whois\` summary when Tailscale is installed
- Optional local \`GET http://127.0.0.1:8080/health\` when braind is running

## Remaining Blockers

- If valkyrie only shows \`RequestTags: [tag:brain]\` locally but \`tailscale whois valkyrie\` still shows a user owner, finish applying \`tag:brain\` in the Tailscale admin UI or re-enroll valkyrie with an auth key scoped to \`tag:brain\`.
- Erbine bootstrap is still intentionally not performed by this handoff flow.

## Single Biggest Remaining Risk

Tailscale local prefs and server-applied tags are different states. A machine can request a tag locally without the server showing it in \`tailscale whois\`. Validate with \`tailscale whois <host>\`, not just \`tailscale debug prefs\`.

## Next Recommended Operator Step

Apply the tag-only Tailscale policy from \`source/infra/tailscale/policy-fragment.example.json\`, remove temporary host/IP fallback grants, then verify:

\`\`\`bash
tailscale whois valkyrie
tailscale whois erbine
ssh erbine true
\`\`\`
EOF

cat > "$bundle_dir/MANIFEST.txt" <<EOF
UTC creation time: $utc
Mode: $mode
Product HEAD: $product_head
Shared brain HEAD: $shared_head
Private dev factory HEAD: $factory_head
Source representation: source/
Secrets included: no
Binaries included: no
EOF

find "$bundle_dir" -type d -empty -delete

source_reps=0
[ -d "$bundle_dir/source" ] && source_reps=$((source_reps + 1))
[ -d "$bundle_dir/patches" ] && source_reps=$((source_reps + 1))
if find "$bundle_dir" -maxdepth 1 -type f \( -name '*.tar' -o -name '*.tar.gz' -o -name '*.tgz' \) | grep -q .; then
  source_reps=$((source_reps + 1))
fi
if [ "$source_reps" -ne 1 ]; then
  echo "handoff refused: expected exactly one source representation, found $source_reps" >&2
  exit 1
fi

for forbidden in ".git" "dist" "node_modules" ".bun" "__MACOSX"; do
  if find "$bundle_dir" -name "$forbidden" -print -quit | grep -q .; then
    echo "handoff refused: forbidden path found: $forbidden" >&2
    exit 1
  fi
done

if find "$bundle_dir" -name '.DS_Store' -print -quit | grep -q .; then
  echo "handoff refused: Finder junk found" >&2
  exit 1
fi

secret_hits="$(
  rg -n \
    -e '-----BEGIN [A-Z ]*PRIVATE KEY-----' \
    -e 'github_pat_[A-Za-z0-9_]{20,}' \
    -e 'gh[pousr]_[A-Za-z0-9_]{20,}' \
    -e '[0-9]{8,12}:[A-Za-z0-9_-]{30,}' \
    -e 'sk-[A-Za-z0-9_-]{30,}' \
    -e 'tskey-[A-Za-z0-9_-]{20,}' \
    "$bundle_dir" || true
)"
if [ -n "$secret_hits" ]; then
  echo "handoff refused: secrets-looking patterns detected" >&2
  echo "$secret_hits" >&2
  exit 1
fi

(cd "$out_dir" && zip -X -qr "$zip_path" "$top" \
  -x '*/.git/*' '*/dist/*' '*/node_modules/*' '*/.bun/*' '*/.DS_Store' '*/__MACOSX/*' \
  -x '*.env' '*.pem' '*.key' '*.token')

sha256sum "$zip_path" > "${zip_path}.sha256"
echo "$zip_path"
cat "${zip_path}.sha256"
