#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/handoff.sh [--mode review|forensic] [--out DIR] [--base COMMIT] [--notes FILE]

Defaults:
  --mode review
  --out  ${TMPDIR:-/tmp}
  --base HEAD^ when available

Environment:
  BRAINSTACK_HANDOFF_UTC=YYYYMMDDTHHMMSSZ  Override bundle timestamp.

The bundle is for review/audit handoff only. It is not a release artifact.
USAGE
}

mode="review"
out_dir="${TMPDIR:-/tmp}"
base_ref=""
notes_file=""

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
    --base)
      base_ref="${2:-}"
      shift 2
      ;;
    --notes)
      notes_file="${2:-}"
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

if [ -n "$notes_file" ] && [ ! -f "$notes_file" ]; then
  echo "handoff refused: notes file not found: $notes_file" >&2
  exit 1
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
if [ -n "$base_ref" ]; then
  if ! base_commit="$(git rev-parse --verify "${base_ref}^{commit}" 2>/dev/null)"; then
    echo "handoff refused: invalid --base commit: $base_ref" >&2
    exit 1
  fi
elif base_commit="$(git rev-parse --verify HEAD^ 2>/dev/null)"; then
  :
else
  base_commit=""
fi
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

{
  echo "# brainstack handoff changes"
  echo
  echo "Base commit: ${base_commit:-none}"
  echo "Head commit: $product_head"
  echo
  if [ -n "$base_commit" ]; then
    echo "## Name Status"
    git diff --name-status "${base_commit}..${product_head}"
    echo
    echo "## Diff Stat"
    git diff --stat "${base_commit}..${product_head}"
  else
    echo "No base commit is available. This appears to be an initial commit handoff."
  fi
} > "$bundle_dir/CHANGES.txt"

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

provision_config="$bundle_dir/generated/provision-client-macos.yaml"
bun run packages/brainctl/src/main.ts provision \
  --profile client-macos \
  --out "$provision_config" \
  --harness codex \
  --brain-base-url https://brain-control.example.ts.net \
  --brain-remote operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git \
  --skip-harness-sudo-test \
  > "$bundle_dir/command-outputs/provision-client-macos.txt" 2>&1

bun run packages/brainctl/src/main.ts destroy \
  --config "$provision_config" \
  --profile client-macos \
  --dry-run \
  > "$bundle_dir/command-outputs/destroy-client-macos-dry-run.txt" 2>&1

{
  echo "product_head=$product_head"
  echo "base_commit=${base_commit:-none}"
  echo "mode=$mode"
  git status --short
} > "$bundle_dir/command-outputs/git-status.txt"

bun test > "$bundle_dir/command-outputs/bun-test.txt" 2>&1

tailscale_ip_for_name() {
  local target="$1"
  tailscale status --json | TAILSCALE_TARGET="$target" bun --eval '
const target = (Bun.env.TAILSCALE_TARGET || "").replace(/\.$/, "");
const data = await new Response(Bun.stdin.stream()).json();
const nodes = [data.Self, ...Object.values(data.Peer || {})].filter(Boolean);
function names(node) {
  return [node.HostName, node.DNSName?.replace(/\.$/, "")].filter(Boolean);
}
function matches(node) {
  return names(node).some((name) => name === target || name.startsWith(`${target}.`) || name.split(".")[0] === target);
}
const node = nodes.find(matches);
if (node?.TailscaleIPs?.length) {
  console.log(node.TailscaleIPs[0]);
}
'
}

if command -v tailscale >/dev/null 2>&1; then
  {
    tailscale status 2>&1 || true
  } > "$bundle_dir/command-outputs/tailscale-summary.txt"

  for host in valkyrie erbine; do
    output="$bundle_dir/command-outputs/tailscale-whois-${host}.txt"
    {
      ip="$(tailscale_ip_for_name "$host" 2>/dev/null || true)"
      if [ -n "$ip" ]; then
        echo "$ tailscale whois $ip"
        tailscale whois "$ip" 2>&1 || true
      else
        echo "No Tailscale IP found for $host in tailscale status --json."
      fi
    } > "$output"
  done
fi

if command -v ssh >/dev/null 2>&1; then
  {
    echo '$ ssh -o BatchMode=yes -o ConnectTimeout=5 erbine true'
    if ssh -o BatchMode=yes -o ConnectTimeout=5 erbine true 2>&1; then
      echo "exit=0"
    else
      status=$?
      echo "exit=$status"
    fi
  } > "$bundle_dir/command-outputs/ssh-erbine-true.txt"
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

cat > "$bundle_dir/CLAIMS_AND_PROOF.md" <<'EOF'
# Claims And Proof

| Claim | Proof |
| --- | --- |
| This bundle includes a real product delta, not just current HEAD. | `CHANGES.txt` |
| The bundle uses exactly one source representation. | `source/` plus `MANIFEST.txt` |
| Client bootstrap respects a custom `client.localPath`. | `generated/client-bootstrap-custom-path/` and `command-outputs/bootstrap-client-custom-path.txt` |
| Worker join paths are derived from config state roots. | `generated/join-worker-custom-state.md` |
| `brainctl provision` generates a discovered first-stage config without installing system packages. | `generated/provision-client-macos.yaml` and `command-outputs/provision-client-macos.txt` |
| `brainctl destroy` has a dry-run teardown plan and does not delete brain repos by default. | `command-outputs/destroy-client-macos-dry-run.txt` |
| Bun tests passed for the product tree at HEAD. | `command-outputs/bun-test.txt` |
| Local braind health was checked when available. | `service-state/braind-health.json` or `service-state/braind-health.txt` |
| Tailscale whois evidence uses Tailscale IPs, not bare hostnames. | `command-outputs/tailscale-whois-valkyrie.txt` and `command-outputs/tailscale-whois-erbine.txt` |
| Valkyrie-to-erbine SSH was smoke-tested without an interactive prompt. | `command-outputs/ssh-erbine-true.txt` |
| Secret-looking tokens and private keys were scanned before zipping. | `command-outputs/secret-scan.txt` |
EOF

{
cat <<EOF
# brainstack ${mode} handoff

This is a REVIEW handoff bundle, not a release artifact.

## Scope Of This Pass

- Generated a review/audit bundle from a clean product HEAD.
- Included generated proof for \`brainctl provision\`, \`brainctl destroy\`, client bootstrap, and worker join.
- Generated this bundle with \`source/\` as the sole source representation.
- Excluded compiled binaries, dist output, dependency trees, git metadata, env files, private keys, tokens, caches, and Finder/macOS junk.

## Pass-Specific Notes

EOF
if [ -n "$notes_file" ]; then
  cat "$notes_file"
else
  echo "No pass-specific notes file was provided."
fi
cat <<EOF

## Exact Changed Files

See \`CHANGES.txt\` for the base commit, head commit, changed files, and diff stat.

## Claims And Proof

See \`CLAIMS_AND_PROOF.md\` for the claim-to-evidence map.

## Valkyrie Production Touches

- This handoff script collects service state only; it does not restart, stop, destroy, or cut over production services.
- Pass-specific notes above are authoritative for whether the implementation pass touched production outside the product repo.

## Exact Validations Run

- \`bun test\`
- \`brainctl bootstrap-client\` with a custom \`client.localPath\`
- \`brainctl join-worker\` with a custom \`paths.stateRoot\`
- \`brainctl provision\` for a generated client-macos config
- \`brainctl destroy --dry-run\` against the generated provision config
- Optional local \`tailscale status/whois\` summary when Tailscale is installed
- Optional local \`GET http://127.0.0.1:8080/health\` when braind is running

## Remaining Blockers

- See pass-specific notes above. This generic handoff script does not infer rollout readiness beyond the generated command outputs.

## Single Biggest Remaining Risk

The bundle is a review artifact, not a release artifact. It proves the checked-in source and selected command outputs at HEAD; it does not prove a fresh external machine has been bootstrapped unless pass-specific notes say that happened.

## Next Recommended Operator Step

Start with \`HANDOFF.md\`, then use \`CHANGES.txt\` and \`CLAIMS_AND_PROOF.md\` to review only the claimed delta and its evidence.
EOF
} > "$bundle_dir/HANDOFF.md"

cat > "$bundle_dir/MANIFEST.txt" <<EOF
UTC creation time: $utc
Mode: $mode
Base commit: ${base_commit:-none}
Product HEAD: $product_head
Shared brain HEAD: $shared_head
Private dev factory HEAD: $factory_head
Pass notes included: $([ -n "$notes_file" ] && echo yes || echo no)
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
{
  echo "No secret-looking patterns detected in bundle directory before zipping."
  echo "Patterns checked: private key headers, GitHub tokens, Telegram bot-token shape, OpenAI sk-* shape, Tailscale tskey-* shape."
} > "$bundle_dir/command-outputs/secret-scan.txt"

(cd "$out_dir" && zip -X -qr "$zip_path" "$top" \
  -x '*/.git/*' '*/dist/*' '*/node_modules/*' '*/.bun/*' '*/.DS_Store' '*/__MACOSX/*' \
  -x '*.env' '*.pem' '*.key' '*.token')

sha256sum "$zip_path" > "${zip_path}.sha256"
echo "$zip_path"
cat "${zip_path}.sha256"
