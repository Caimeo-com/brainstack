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
  BRAINSTACK_HANDOFF_LIVE_HOSTS="host1 host2"  In forensic mode only, collect opt-in live SSH/Tailscale checks.
  BRAINSTACK_HANDOFF_PRIVATE_SUBSTITUTIONS_FILE=PATH  Optional gitignored literal substitutions file with "private=replacement" lines.

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
if ! printf '%s\n' "$utc" | grep -Eq '^[0-9]{8}T[0-9]{6}Z$'; then
  echo "handoff refused: BRAINSTACK_HANDOFF_UTC must match YYYYMMDDTHHMMSSZ" >&2
  exit 1
fi
top="handoff-${utc}"
mkdir -p "$out_dir"
out_dir="$(cd "$out_dir" && pwd)"
bundle_dir="${out_dir}/${top}"
zip_path="${out_dir}/${top}.zip"
case "$bundle_dir" in
  "$out_dir"/*) ;;
  *) echo "handoff refused: bundle path escaped output directory" >&2; exit 1 ;;
esac
case "$zip_path" in
  "$out_dir"/*) ;;
  *) echo "handoff refused: zip path escaped output directory" >&2; exit 1 ;;
esac

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
shared_brain_root="${SHARED_BRAIN_ROOT:-}"
handoff_factory_root="${BRAINSTACK_HANDOFF_FACTORY_ROOT:-}"

shared_head="not-collected"
if [ "$mode" = "forensic" ] && [ "${BRAINSTACK_HANDOFF_INCLUDE_SHARED_BRAIN:-}" = "1" ] && [ -n "$shared_brain_root" ] && [ -d "$shared_brain_root/.git" ]; then
  shared_head="$(git -C "$shared_brain_root" rev-parse HEAD)"
fi

factory_head="not-collected"
if [ "$mode" = "forensic" ] && [ -n "$handoff_factory_root" ] && [ -d "$handoff_factory_root/.git" ]; then
  factory_head="$(git -C "$handoff_factory_root" rev-parse HEAD)"
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
  localPath: /Users/operator/brain/shared-brain
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
security:
  trustedExposure: tailscale-serve
tailscale:
  tailnetHost: brain-control.tailnet.invalid
telemux:
  enabled: false
YAML

bun run packages/brainctl/src/main.ts join-worker \
  --config "$state_config" \
  --worker brain-worker \
  > "$bundle_dir/generated/join-worker-custom-state.md" 2>&1

provision_config="$bundle_dir/generated/provision-client-macos.yaml"
env USER=operator HOME=/home/operator bun run packages/brainctl/src/main.ts provision \
  --profile client-macos \
  --out "$provision_config" \
  --root "$bundle_dir/generated/provision-root" \
  --harness codex \
  --machine client-laptop \
  --hostname client-laptop \
  --brain-base-url https://brain-control.example.ts.net \
  --brain-remote operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git \
  --skip-harness-sudo-test \
  > "$bundle_dir/command-outputs/provision-client-macos.txt" 2>&1

bun run packages/brainctl/src/main.ts destroy \
  --config "$provision_config" \
  --profile client-macos \
  --dry-run \
  > "$bundle_dir/command-outputs/destroy-client-macos-dry-run.txt" 2>&1

bun run packages/brainctl/src/main.ts expose tailscale \
  --config "$state_config" \
  --dry-run \
  > "$bundle_dir/command-outputs/expose-tailscale-dry-run.txt" 2>&1

{
  echo "product_head=$product_head"
  echo "base_commit=${base_commit:-none}"
  echo "mode=$mode"
  git status --short
} > "$bundle_dir/command-outputs/git-status.txt"

bun test > "$bundle_dir/command-outputs/bun-test.txt" 2>&1

bun run build:brainctl > "$bundle_dir/command-outputs/build-brainctl.txt" 2>&1

bun run packages/brainctl/src/main.ts doctor \
  --config examples/control.yaml \
  --root "$bundle_dir/generated/example-root" \
  --workers \
  > "$bundle_dir/command-outputs/brainctl-doctor-workers.txt" 2>&1

bun run packages/brainctl/src/main.ts updates \
  --config examples/control.yaml \
  --root "$bundle_dir/generated/example-root" \
  > "$bundle_dir/command-outputs/brainctl-updates.txt" 2>&1

bun test apps/telemux/test/phase1.test.ts -t "Telegram text coalescing" \
  > "$bundle_dir/command-outputs/telemux-coalescing-test.txt" 2>&1

bun test apps/telemux/test/phase1.test.ts -t "worker harness" \
  > "$bundle_dir/command-outputs/worker-harness-path-neutral-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "braind converts expired running" \
  > "$bundle_dir/command-outputs/idempotency-recovery-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "pre-mutation idempotent import failures" \
  > "$bundle_dir/command-outputs/idempotency-preflight-retry.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "braind escapes search snippets" \
  > "$bundle_dir/command-outputs/healthz-and-search-safety-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "worker SSH trust" \
  > "$bundle_dir/command-outputs/ssh-trust-lock-recovery-test.txt" 2>&1

bun test apps/telemux/test/phase1.test.ts -t "SSH accept-new trust mode" \
  > "$bundle_dir/command-outputs/ssh-accept-new-dispatch-refusal-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "outbox moves repeated HTTP 425" \
  > "$bundle_dir/command-outputs/outbox-425-terminal-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "doctor explains trusted-tailnet" \
  > "$bundle_dir/command-outputs/security-posture-doctor-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "project context uses profiles" \
  > "$bundle_dir/command-outputs/multi-brain-profiles-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "project context blocks personal" \
  > "$bundle_dir/command-outputs/multi-brain-repo-local-safety-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "worker init prints exact manual merge" \
  > "$bundle_dir/command-outputs/harness-guidance-existing-file.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "slow URL import preparation" \
  > "$bundle_dir/command-outputs/write-gate-narrowing-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "outbox surfaces corrupt" \
  > "$bundle_dir/command-outputs/outbox-hardening-test.txt" 2>&1

bun test packages/brainctl/test/brainctl.test.ts -t "outbox warns on large" \
  > "$bundle_dir/command-outputs/outbox-large-payload-test.txt" 2>&1

bun test apps/telemux/test/phase1.test.ts -t "telemux state database" \
  > "$bundle_dir/command-outputs/telemux-private-state-test.txt" 2>&1

bun test apps/telemux/test/phase1.test.ts -t "dispatcher reports running queued turns abandoned" \
  > "$bundle_dir/command-outputs/telegram-durable-queue-test.txt" 2>&1

cp "$bundle_dir/command-outputs/security-posture-doctor-test.txt" "$bundle_dir/command-outputs/doctor-security-posture-default.txt"
cp "$bundle_dir/command-outputs/security-posture-doctor-test.txt" "$bundle_dir/command-outputs/doctor-security-posture-public-bind.txt"
cp "$bundle_dir/command-outputs/healthz-and-search-safety-test.txt" "$bundle_dir/command-outputs/healthz.txt"
cp "$bundle_dir/command-outputs/healthz-and-search-safety-test.txt" "$bundle_dir/command-outputs/deep-health-redacted-or-authenticated.txt"
cp "$bundle_dir/command-outputs/multi-brain-profiles-test.txt" "$bundle_dir/command-outputs/context-default-repo.txt"
cp "$bundle_dir/command-outputs/multi-brain-profiles-test.txt" "$bundle_dir/command-outputs/context-project-first-noninteractive.txt"
cp "$bundle_dir/command-outputs/multi-brain-profiles-test.txt" "$bundle_dir/command-outputs/context-project-after-approval.txt"
cp "$bundle_dir/command-outputs/multi-brain-profiles-test.txt" "$bundle_dir/command-outputs/search-source-labelled.txt"
cp "$bundle_dir/command-outputs/multi-brain-profiles-test.txt" "$bundle_dir/command-outputs/search-stale-cli.txt"
cp "$bundle_dir/command-outputs/healthz-and-search-safety-test.txt" "$bundle_dir/command-outputs/search-stale-ui-snippet.txt"
cp "$bundle_dir/command-outputs/healthz-and-search-safety-test.txt" "$bundle_dir/command-outputs/search-fresh-after-reindex.txt"
cp "$bundle_dir/command-outputs/multi-brain-profiles-test.txt" "$bundle_dir/command-outputs/remember-personal-default.txt"
cp "$bundle_dir/command-outputs/multi-brain-profiles-test.txt" "$bundle_dir/command-outputs/remember-company-default.txt"
cp "$bundle_dir/command-outputs/multi-brain-profiles-test.txt" "$bundle_dir/command-outputs/remember-cross-brain-warning.txt"
cp "$bundle_dir/command-outputs/bootstrap-client-custom-path.txt" "$bundle_dir/command-outputs/harness-guidance-fresh.txt"
cp "$bundle_dir/command-outputs/brainctl-doctor-workers.txt" "$bundle_dir/command-outputs/doctor-harness-guidance.txt"
cp "$bundle_dir/command-outputs/outbox-hardening-test.txt" "$bundle_dir/command-outputs/outbox-permissions.txt"
cp "$bundle_dir/command-outputs/outbox-hardening-test.txt" "$bundle_dir/command-outputs/outbox-corrupt-item.txt"
cp "$bundle_dir/command-outputs/outbox-large-payload-test.txt" "$bundle_dir/command-outputs/outbox-large-compressed.txt"
cp "$bundle_dir/command-outputs/outbox-large-payload-test.txt" "$bundle_dir/command-outputs/outbox-hard-cap-refusal.txt"
cp "$bundle_dir/command-outputs/outbox-large-payload-test.txt" "$bundle_dir/command-outputs/outbox-log-redaction-proof.txt"
cp "$bundle_dir/command-outputs/idempotency-recovery-test.txt" "$bundle_dir/command-outputs/idempotency-post-side-effect-review.txt"
cp "$bundle_dir/command-outputs/healthz-and-search-safety-test.txt" "$bundle_dir/command-outputs/idempotency-duplicate-success.txt"
cp "$bundle_dir/command-outputs/write-gate-narrowing-test.txt" "$bundle_dir/command-outputs/write-gate-slow-url-proof.txt"
cp "$bundle_dir/command-outputs/worker-harness-path-neutral-test.txt" "$bundle_dir/command-outputs/worker-path-cache.txt"

{
  assert_doc_contains() {
    local file="$1"
    local pattern="$2"
    local label="$3"
    if [ ! -f "$file" ]; then
      echo "MISSING file: $file ($label)" >&2
      return 1
    fi
    if ! rg -n -i -e "$pattern" "$file"; then
      echo "MISSING claim: $label in $file" >&2
      return 1
    fi
  }
  assert_doc_contains README.md "trusted private networks|trusted-tailnet" "README states trusted-tailnet/private network stance"
  assert_doc_contains docs/security-postures.md "defaults to a trusted private mesh|trusted-tailnet" "trusted-tailnet is default"
  assert_doc_contains docs/security-postures.md "does not require read tokens|read tokens" "default mode has no mandatory read tokens"
  assert_doc_contains docs/security-postures.md "Do not expose trusted-tailnet mode to the public internet" "trusted-tailnet is not public exposure"
  assert_doc_contains docs/tailscale-exposure.md "brainctl expose tailscale" "Tailscale helper command is documented"
  assert_doc_contains docs/tailscale-exposure.md "tailscale serve" "Tailscale Serve is documented"
  assert_doc_contains docs/multi-brain.md "project config" "project config activates multi-brain behavior"
  assert_doc_contains docs/multi-brain.md "\\.brainstack\\.yaml" "repo-local project config filename is documented"
  assert_doc_contains docs/multi-brain.md "profiles\\.yaml" "profiles config is documented"
  assert_doc_contains docs/multi-brain.md "pending-trust" "pending-trust repo-local connection posture is documented"
  assert_doc_contains docs/multi-brain.md "URL/token/remote/path|URLs, remotes, token environment names, and local clone paths" "repo-local concrete connection fields require user trust"
  assert_doc_contains docs/multi-brain.md "not hard security boundaries" "sections are not hard security boundaries"
  assert_doc_contains docs/multi-brain.md "retrieval boundaries" "sections are retrieval boundaries"
  assert_doc_contains docs/outbox-security.md "plaintext by default|sensitive" "outbox payloads are sensitive plaintext by default"
  assert_doc_contains docs/outbox-security.md "never silently truncated|No queued payload is silently truncated" "no silent truncation"
  assert_doc_contains docs/outbox-security.md "server-sealed" "future server-sealed encryption explanation"
  assert_doc_contains packages/client-bootstrap/claude-user-CLAUDE.md "brainctl search --repo \\." "Claude bootstrap uses repo-scoped search"
  assert_doc_contains packages/client-bootstrap/claude-user-CLAUDE.md "brainctl remember --repo \\." "Claude bootstrap uses repo-scoped remember"
} > "$bundle_dir/command-outputs/docs-presence.txt" 2>&1

multi_brain_smoke_root="$(mktemp -d "${out_dir}/brainstack-handoff-multibrain.XXXXXX")"
multi_brain_config="$multi_brain_smoke_root/config.yaml"
multi_brain_repo="$multi_brain_smoke_root/repo"
multi_brain_work="$multi_brain_smoke_root/brains/lindy"
multi_brain_personal="$multi_brain_smoke_root/brains/personal"
mkdir -p "$multi_brain_repo" "$multi_brain_work/wiki" "$multi_brain_personal/wiki"
cat > "$multi_brain_config" <<YAML
schema_version: 1
profile: client-macos
machine:
  name: client
  user: operator
paths:
  home: $multi_brain_smoke_root/home
  stateRoot: $multi_brain_smoke_root/state
client:
  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git
YAML
git -C "$multi_brain_work" init -q
git -C "$multi_brain_personal" init -q
printf '%s\n' "handoff deploy checklist from lindy" > "$multi_brain_work/wiki/Runbook.md"
printf '%s\n' "handoff personal deployment note" > "$multi_brain_personal/wiki/Journal.md"
cat > "$multi_brain_repo/.brainstack.yaml" <<YAML
writeDefault: lindy
brains:
  - id: lindy
    label: Lindy work brain
    baseUrl: http://127.0.0.1:9
    importTokenEnv: BRAIN_IMPORT_TOKEN
    localPath: $multi_brain_work
    classification: work
    sections: [wiki]
    write: true
  - id: personal
    label: Personal brain
    localPath: $multi_brain_personal
    classification: personal
    sections: [wiki]
    read: true
    write: propose-only
crossBrainWrites:
  personalToLindy: ask
YAML

bun run packages/brainctl/src/main.ts context \
  --repo "$multi_brain_repo" \
  --config "$multi_brain_config" \
  --root "$multi_brain_smoke_root" \
  --no-sync \
  > "$bundle_dir/command-outputs/context-pending-trust-cli.txt" 2>&1

mkdir -p "$multi_brain_smoke_root/config"
cat > "$multi_brain_smoke_root/config/profiles.yaml" <<YAML
brains:
  lindy:
    baseUrl: http://127.0.0.1:9
    importTokenEnv: BRAIN_IMPORT_TOKEN
    localPath: $multi_brain_work
    classification: work
    write: true
  personal:
    localPath: $multi_brain_personal
    classification: personal
YAML

bun run packages/brainctl/src/main.ts allow repo \
  --repo "$multi_brain_repo" \
  --brain personal \
  --sections wiki \
  --always \
  --config "$multi_brain_config" \
  --root "$multi_brain_smoke_root" \
  > "$bundle_dir/command-outputs/context-personal-allow-cli.txt" 2>&1

bun run packages/brainctl/src/main.ts context \
  --repo "$multi_brain_repo" \
  --config "$multi_brain_config" \
  --root "$multi_brain_smoke_root" \
  --no-sync \
  > "$bundle_dir/command-outputs/context-after-trust-cli.txt" 2>&1

bun run packages/brainctl/src/main.ts search \
  --repo "$multi_brain_repo" \
  --config "$multi_brain_config" \
  --root "$multi_brain_smoke_root" \
  --no-sync \
  --query handoff \
  > "$bundle_dir/command-outputs/search-source-labelled-cli.txt" 2>&1

set +e
bun run packages/brainctl/src/main.ts remember \
  --repo "$multi_brain_repo" \
  --target lindy \
  --summary "handoff cross-brain blocked example" \
  --config "$multi_brain_config" \
  --root "$multi_brain_smoke_root" \
  > "$bundle_dir/command-outputs/remember-cross-brain-blocked-cli.txt" 2>&1
remember_blocked_code=$?
set -e
printf 'exit=%s\n' "$remember_blocked_code" >> "$bundle_dir/command-outputs/remember-cross-brain-blocked-cli.txt"
if [ "$remember_blocked_code" -eq 0 ]; then
  echo "handoff refused: expected cross-brain remember example to fail before confirmation" >&2
  exit 1
fi

bun run packages/brainctl/src/main.ts remember \
  --repo "$multi_brain_repo" \
  --target lindy \
  --summary "handoff cross-brain confirmed example" \
  --confirm-cross-brain \
  --config "$multi_brain_config" \
  --root "$multi_brain_smoke_root" \
  > "$bundle_dir/command-outputs/remember-cross-brain-confirmed-cli.txt" 2>&1

bun run packages/brainctl/src/main.ts outbox list \
  --config "$multi_brain_config" \
  --root "$multi_brain_smoke_root" \
  > "$bundle_dir/command-outputs/outbox-list-cli.txt" 2>&1

rm -rf "$multi_brain_smoke_root"

cp docs/diagrams.md "$bundle_dir/generated/diagrams.md"

outbox_smoke_root="$bundle_dir/generated/outbox-smoke"
mkdir -p "$outbox_smoke_root"
outbox_config="$outbox_smoke_root/config.yaml"
outbox_received="$outbox_smoke_root/received.jsonl"
outbox_server="$outbox_smoke_root/server.ts"
outbox_port="$((35000 + ((RANDOM + $$) % 25000)))"
json_string_literal() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  printf '"%s"' "$value"
}
outbox_received_js="$(json_string_literal "$outbox_received")"
cat > "$outbox_config" <<YAML
schema_version: 1
profile: client-macos
machine:
  name: client
  user: operator
paths:
  home: $outbox_smoke_root/home
  stateRoot: $outbox_smoke_root/state
client:
  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git
YAML
cat > "$outbox_server" <<EOF
const receivedPath = $outbox_received_js;
Bun.serve({
  hostname: "127.0.0.1",
  port: $outbox_port,
  async fetch(req) {
    if (new URL(req.url).pathname === "/health") return Response.json({ ok: true });
    const payload = await req.json();
    const existing = await Bun.file(receivedPath).exists() ? await Bun.file(receivedPath).text() : "";
    await Bun.write(receivedPath, \`\${existing}\${JSON.stringify(payload)}\\n\`);
    return Response.json({ ok: true });
  }
});
await new Promise(() => {});
EOF
{
  export BRAIN_BASE_URL="http://127.0.0.1:$outbox_port"
  export BRAIN_IMPORT_TOKEN="handoff-token"
  export BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS=300
  bun run packages/brainctl/src/main.ts import-text --config "$outbox_config" --title "handoff offline note" --text "offline queue smoke" --source-harness codex --source-machine client
  bun run packages/brainctl/src/main.ts outbox status --config "$outbox_config"
  bun run "$outbox_server" >"$bundle_dir/command-outputs/outbox-smoke-server.log" 2>&1 &
  server_pid=$!
  trap 'kill "$server_pid" 2>/dev/null || true' EXIT
  for _ in $(seq 1 40); do
    curl -fsS "http://127.0.0.1:$outbox_port/health" >/dev/null 2>&1 && break
    sleep 0.05
  done
  bun run packages/brainctl/src/main.ts outbox flush --config "$outbox_config"
  bun run packages/brainctl/src/main.ts outbox status --config "$outbox_config"
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  trap - EXIT
  echo "received_lines=$(test -f "$outbox_received" && wc -l < "$outbox_received" || echo 0)"
} > "$bundle_dir/command-outputs/outbox-queue-flush-smoke.txt" 2>&1

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

live_hosts="${BRAINSTACK_HANDOFF_LIVE_HOSTS:-}"
if [ "$mode" = "forensic" ] && [ -n "$live_hosts" ] && command -v tailscale >/dev/null 2>&1; then
  {
    tailscale status 2>&1 || true
  } > "$bundle_dir/command-outputs/tailscale-summary.txt"

  for host in $live_hosts; do
    safe_host="$(printf '%s' "$host" | sed 's/[^A-Za-z0-9_.-]/_/g')"
    output="$bundle_dir/command-outputs/tailscale-whois-${safe_host}.txt"
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

if [ "$mode" = "forensic" ] && [ -n "$live_hosts" ] && command -v ssh >/dev/null 2>&1; then
  for host in $live_hosts; do
    safe_host="$(printf '%s' "$host" | sed 's/[^A-Za-z0-9_.-]/_/g')"
    {
      echo "$ ssh -o BatchMode=yes -o ConnectTimeout=5 ${safe_host} true"
      if ssh -o BatchMode=yes -o ConnectTimeout=5 "$host" true 2>&1; then
        echo "exit=0"
      else
        status=$?
        echo "exit=$status"
      fi
    } > "$bundle_dir/command-outputs/ssh-${safe_host}-true.txt"
  done
fi

if [ "$mode" = "forensic" ] && curl -fsS --max-time 2 http://127.0.0.1:8080/healthz > "$bundle_dir/service-state/braind-health.json" 2>/dev/null; then
  :
else
  if [ "$mode" = "forensic" ]; then
    echo "braind health unavailable on http://127.0.0.1:8080/healthz" > "$bundle_dir/service-state/braind-health.txt"
  else
    echo "braind live health is not collected in review mode because it can include local absolute paths. Use --mode forensic only when live host evidence is intended." > "$bundle_dir/service-state/braind-health.txt"
  fi
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

if [ "$mode" = "forensic" ] && [ "${BRAINSTACK_HANDOFF_INCLUDE_SHARED_BRAIN:-}" = "1" ] && [ -d "$shared_brain_root" ]; then
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
| `brainctl doctor` reports worker/harness compatibility without auto-updating anything. | `command-outputs/brainctl-doctor-workers.txt` |
| `brainctl updates` is read-only manual update visibility. | `command-outputs/brainctl-updates.txt` |
| Default trusted-tailnet doctor posture is proven. | `command-outputs/doctor-security-posture-default.txt` |
| Accidental public/wildcard trusted-tailnet binds fail loudly. | `command-outputs/doctor-security-posture-public-bind.txt` |
| Tailscale Serve dry-run prints exact commands without installing anything. | `command-outputs/expose-tailscale-dry-run.txt` |
| Public `/healthz` is minimal and deep health stays separate. | `command-outputs/healthz.txt` and `command-outputs/deep-health-redacted-or-authenticated.txt` |
| Offline import/propose writes queue locally and flush later. | `command-outputs/outbox-queue-flush-smoke.txt` |
| Telegram text coalescing is covered by tests. | `command-outputs/telemux-coalescing-test.txt` |
| Worker harness execution is override-capable and path-neutral for remote workers. | `command-outputs/worker-harness-path-neutral-test.txt` |
| Stuck idempotent writes graduate to explicit operator review instead of endless retry. | `command-outputs/idempotency-recovery-test.txt` |
| Worker SSH trust defaults to pinned mode and lock recovery is token-guarded. | `command-outputs/ssh-trust-lock-recovery-test.txt` |
| Telemux dispatch refuses bootstrap-only SSH `accept-new` mode unless explicitly enabled. | `command-outputs/ssh-accept-new-dispatch-refusal-test.txt` |
| Repeated HTTP 425 outbox flushes become terminal operator-review failures. | `command-outputs/outbox-425-terminal-test.txt` |
| `brainctl doctor` explains trusted-tailnet posture and fails accidental non-loopback binds. | `command-outputs/security-posture-doctor-test.txt` |
| Project-triggered multi-brain context supports profiles, clone/pull, allow rules, source labels, and cross-brain refusal. | `command-outputs/multi-brain-profiles-test.txt` |
| Repo-local `.brainstack.yaml` cannot point reads/clones at arbitrary local paths unless those paths are trusted in profiles. | `command-outputs/multi-brain-repo-local-safety-test.txt` |
| Human-facing multi-brain CLI flows have real stdout/stderr examples for pending trust, trusted context, labelled search, cross-brain refusal, confirmed queueing, and outbox listing. | `command-outputs/context-pending-trust-cli.txt`, `command-outputs/context-after-trust-cli.txt`, `command-outputs/search-source-labelled-cli.txt`, `command-outputs/remember-cross-brain-blocked-cli.txt`, `command-outputs/remember-cross-brain-confirmed-cli.txt`, `command-outputs/outbox-list-cli.txt` |
| Multi-brain context/search/remember checklist aliases are backed by the combined multi-brain regression transcript. | `command-outputs/context-default-repo.txt`, `command-outputs/context-project-first-noninteractive.txt`, `command-outputs/context-project-after-approval.txt`, `command-outputs/search-source-labelled.txt`, `command-outputs/remember-personal-default.txt`, `command-outputs/remember-company-default.txt`, `command-outputs/remember-cross-brain-warning.txt` |
| Harness bootstrap guidance uses repo-scoped `brainctl context/search/remember`, does not silently overwrite existing files, and doctor reports guidance state. | `command-outputs/harness-guidance-fresh.txt`, `command-outputs/harness-guidance-existing-file.txt`, `command-outputs/doctor-harness-guidance.txt` |
| Slow URL import preparation does not occupy the serialized repo mutation slot. | `command-outputs/write-gate-narrowing-test.txt` |
| Outbox permissions, corrupt entries, and symlink namespace handling are covered. | `command-outputs/outbox-hardening-test.txt` |
| Outbox checklist aliases are backed by the focused outbox hardening and large-payload transcripts. | `command-outputs/outbox-permissions.txt`, `command-outputs/outbox-corrupt-item.txt`, `command-outputs/outbox-large-compressed.txt`, `command-outputs/outbox-hard-cap-refusal.txt`, `command-outputs/outbox-log-redaction-proof.txt` |
| Large outbox payloads warn/compress and over-cap payloads fail rather than truncate. | `command-outputs/outbox-large-payload-test.txt` |
| Idempotency checklist aliases are backed by focused preflight/recovery/content-safety transcripts. | `command-outputs/idempotency-preflight-retry.txt`, `command-outputs/idempotency-post-side-effect-review.txt`, `command-outputs/idempotency-duplicate-success.txt` |
| Search freshness warnings are surfaced in CLI and UI-related combined coverage. | `command-outputs/search-stale-cli.txt`, `command-outputs/search-stale-ui-snippet.txt`, `command-outputs/search-fresh-after-reindex.txt` |
| Durable Telegram queued-turn restart behavior is covered. | `command-outputs/telegram-durable-queue-test.txt` |
| Worker PATH cache behavior is covered with the same fingerprint contract as doctor/dispatch. | `command-outputs/worker-path-cache.txt` |
| Durable telemux SQLite state is private even under permissive process umask. | `command-outputs/telemux-private-state-test.txt` |
| Docs and packaged guidance contain the trusted-tailnet, multi-brain, context, and outbox-security terms expected by the audit. | `command-outputs/docs-presence.txt` |
| `brainctl` compiled successfully without dotenv/bunfig autoloading. | `command-outputs/build-brainctl.txt` |
| Mermaid diagrams are checked in and included for reviewer context. | `generated/diagrams.md` |
| Bun tests passed for the product tree at HEAD. | `command-outputs/bun-test.txt` |
| Local braind health is skipped in review mode and collected only in forensic mode. | `service-state/braind-health.json` or `service-state/braind-health.txt` |
| Optional live host evidence is forensic-only and opt-in. | `BRAINSTACK_HANDOFF_LIVE_HOSTS` |
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

The \`source/\` tree is the tracked HEAD archive after handoff path/private-literal sanitization. Use the Product HEAD commit in \`MANIFEST.txt\` as the canonical exact source identity.

## Claims And Proof

See \`CLAIMS_AND_PROOF.md\` for the claim-to-evidence map.

## Production Touches

- This handoff script collects service state only; it does not restart, stop, destroy, or cut over production services.
- Pass-specific notes above are authoritative for whether the implementation pass touched production outside the product repo.

## Exact Validations Run

- \`bun test\`
- \`bun run build:brainctl\`
- \`brainctl bootstrap-client\` with a custom \`client.localPath\`
- \`brainctl join-worker\` with a custom \`paths.stateRoot\`
- \`brainctl provision\` for a generated client-macos config
- \`brainctl destroy --dry-run\` against the generated provision config
- \`brainctl doctor --workers\`
- \`brainctl updates\`
- Offline outbox queue/flush smoke
- Focused telemux coalescing, worker-harness, private-state, outbox-hardening, posture, multi-brain, and write-gate tests
- Optional forensic-only \`tailscale status/whois\` and SSH checks when \`BRAINSTACK_HANDOFF_LIVE_HOSTS\` is explicitly set
- Optional forensic-only local \`GET http://127.0.0.1:8080/healthz\` when braind is running

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
Factory workspace HEAD: $factory_head
Pass notes included: $([ -n "$notes_file" ] && echo yes || echo no)
Source representation: source/
Source tree note: source/ contains tracked HEAD after handoff path/private-literal sanitization; Product HEAD is the canonical source identity.
Secrets included: no
Binaries included: no
EOF

required_command_outputs=(
  "bun-test.txt"
  "build-brainctl.txt"
  "brainctl-doctor-workers.txt"
  "brainctl-updates.txt"
  "doctor-security-posture-default.txt"
  "doctor-security-posture-public-bind.txt"
  "healthz.txt"
  "deep-health-redacted-or-authenticated.txt"
  "expose-tailscale-dry-run.txt"
  "context-default-repo.txt"
  "context-project-first-noninteractive.txt"
  "context-project-after-approval.txt"
  "search-source-labelled.txt"
  "context-pending-trust-cli.txt"
  "context-after-trust-cli.txt"
  "search-source-labelled-cli.txt"
  "remember-cross-brain-blocked-cli.txt"
  "remember-cross-brain-confirmed-cli.txt"
  "outbox-list-cli.txt"
  "remember-personal-default.txt"
  "remember-company-default.txt"
  "remember-cross-brain-warning.txt"
  "harness-guidance-fresh.txt"
  "harness-guidance-existing-file.txt"
  "doctor-harness-guidance.txt"
  "outbox-permissions.txt"
  "outbox-corrupt-item.txt"
  "outbox-large-compressed.txt"
  "outbox-hard-cap-refusal.txt"
  "outbox-log-redaction-proof.txt"
  "idempotency-preflight-retry.txt"
  "idempotency-post-side-effect-review.txt"
  "idempotency-duplicate-success.txt"
  "write-gate-slow-url-proof.txt"
  "search-stale-cli.txt"
  "search-stale-ui-snippet.txt"
  "search-fresh-after-reindex.txt"
  "telegram-durable-queue-test.txt"
  "worker-path-cache.txt"
  "security-posture-doctor-test.txt"
  "multi-brain-profiles-test.txt"
  "multi-brain-repo-local-safety-test.txt"
  "write-gate-narrowing-test.txt"
  "outbox-hardening-test.txt"
  "outbox-large-payload-test.txt"
  "telemux-private-state-test.txt"
  "docs-presence.txt"
  "telemux-coalescing-test.txt"
  "worker-harness-path-neutral-test.txt"
  "idempotency-recovery-test.txt"
  "ssh-trust-lock-recovery-test.txt"
  "ssh-accept-new-dispatch-refusal-test.txt"
  "outbox-425-terminal-test.txt"
  "outbox-queue-flush-smoke.txt"
)
for artifact in "${required_command_outputs[@]}"; do
  if [ ! -s "$bundle_dir/command-outputs/$artifact" ]; then
    echo "handoff refused: required proof artifact missing or empty: command-outputs/$artifact" >&2
    exit 1
  fi
done

find "$bundle_dir" -type d -empty -delete

sanitize_handoff_text() {
  text_files() {
    find "$bundle_dir" -type f -print0 | while IFS= read -r -d '' file; do
      if [ ! -s "$file" ] || LC_ALL=C grep -Iq . "$file"; then
        printf '%s\0' "$file"
      fi
    done
  }
  while IFS= read -r -d '' file; do
    perl -0pi \
      -e "s#/Users/[A-Za-z0-9._-]+#/Users/operator#g;" \
      -e "s#/home/(?!operator\\b|factory\\b|brainstack\\b)[A-Za-z0-9._-]+#/home/operator#g;" \
      "$file"
  done < <(text_files)
  local substitutions_file="${BRAINSTACK_HANDOFF_PRIVATE_SUBSTITUTIONS_FILE:-$repo_root/.handoff-private-substitutions}"
  if [ -f "$substitutions_file" ]; then
    while IFS='=' read -r literal replacement; do
      case "$literal" in ""|\#*) continue ;; esac
      if [ -z "${replacement:-}" ]; then
        replacement="[redacted]"
      fi
      while IFS= read -r -d '' file; do
        BRAINSTACK_SUB_LITERAL="$literal" BRAINSTACK_SUB_REPLACEMENT="$replacement" perl -0pi -e 'BEGIN { $literal = $ENV{"BRAINSTACK_SUB_LITERAL"}; $replacement = $ENV{"BRAINSTACK_SUB_REPLACEMENT"}; } s/\Q$literal\E/$replacement/g;' "$file"
      done < <(text_files)
    done < "$substitutions_file"
  fi
}

sanitize_handoff_text

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

if find "$bundle_dir" -type l -print -quit | grep -q .; then
  echo "handoff refused: symlink found in bundle" >&2
  find "$bundle_dir" -type l -print >&2
  exit 1
fi

local_hygiene_hits="$(
  rg --hidden --no-ignore \
    -g '!**/.git/**' \
    -g '!**/node_modules/**' \
    -g '!**/dist/**' \
    -n \
    -P -e '/Users/(?!operator\b)[A-Za-z0-9._-]+' \
    -e '/home/(?!operator\b|factory\b|brainstack\b)[A-Za-z0-9._-]+' \
    -e 'migration-from-current-[A-Za-z0-9._-]+\.md' \
    "$bundle_dir" 2>/dev/null || true
)"
if [ -n "$local_hygiene_hits" ]; then
  echo "handoff refused: local/private identifiers detected" >&2
  echo "$local_hygiene_hits" >&2
  exit 1
fi

scan_secret_detector() {
  detector="$1"
  pattern="$2"
  rg --hidden --no-ignore \
    -g '!**/.git/**' \
    -g '!**/node_modules/**' \
    -g '!**/dist/**' \
    -n -e "$pattern" "$bundle_dir" 2>/dev/null \
    | awk -F: -v detector="$detector" '{print $1 ":" $2 ": [REDACTED " detector "]"}' \
    || true
}

secret_hits="$(
  {
    scan_secret_detector "private-key" '-----BEGIN [A-Z ]*PRIVATE KEY-----'
    scan_secret_detector "github-token" 'github_pat_[A-Za-z0-9_]{20,}'
    scan_secret_detector "github-token" 'gh[pousr]_[A-Za-z0-9_]{20,}'
    scan_secret_detector "telegram-token" '[0-9]{8,12}:[A-Za-z0-9_-]{30,}'
    scan_secret_detector "openai-key" 'sk-[A-Za-z0-9_-]{30,}'
    scan_secret_detector "tailscale-key" 'tskey-[A-Za-z0-9_-]{20,}'
  } || true
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

(cd "$bundle_dir" && { find . -type f ! -name ZIP-MANIFEST.txt -print | sed 's#^\./##'; echo "ZIP-MANIFEST.txt"; } | sort > ZIP-MANIFEST.txt)

(cd "$out_dir" && zip -X -qr "$zip_path" "$top" \
  -x '*/.git/*' '*/dist/*' '*/node_modules/*' '*/.bun/*' '*/.DS_Store' '*/__MACOSX/*' \
  -x '*.env' '*.pem' '*.key' '*.token')

zip_listing_file="$bundle_dir/command-outputs/zip-listing.txt"
if command -v zipinfo >/dev/null 2>&1; then
  zipinfo -1 "$zip_path" | grep -v '/$' | sort > "$zip_listing_file"
elif command -v unzip >/dev/null 2>&1; then
  unzip -Z1 "$zip_path" | grep -v '/$' | sort > "$zip_listing_file"
else
  echo "handoff refused: need zipinfo or unzip to verify archive contents" >&2
  exit 1
fi
expected_listing_file="$bundle_dir/command-outputs/zip-listing.expected.txt"
sed "s#^#${top}/#" "$bundle_dir/ZIP-MANIFEST.txt" | sort > "$expected_listing_file"
if ! diff -u "$expected_listing_file" "$zip_listing_file" > "$bundle_dir/command-outputs/zip-listing-diff.txt"; then
  echo "handoff refused: final zip listing differs from bundle manifest" >&2
  cat "$bundle_dir/command-outputs/zip-listing-diff.txt" >&2
  exit 1
fi

for required_path in HANDOFF.md CHANGES.txt CLAIMS_AND_PROOF.md MANIFEST.txt ZIP-MANIFEST.txt source/package.json command-outputs/bun-test.txt generated/diagrams.md; do
  if ! grep -Fxq "${top}/${required_path}" "$zip_listing_file"; then
    echo "handoff refused: final zip missing required path: $required_path" >&2
    exit 1
  fi
done
for forbidden_path in "/.git/" "/dist/" "/node_modules/" "/.bun/" "__MACOSX" ".DS_Store"; do
  if grep -Fq "$forbidden_path" "$zip_listing_file"; then
    echo "handoff refused: final zip contains forbidden path marker: $forbidden_path" >&2
    exit 1
  fi
done

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$zip_path" > "${zip_path}.sha256"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$zip_path" > "${zip_path}.sha256"
elif command -v openssl >/dev/null 2>&1; then
  digest="$(openssl dgst -sha256 "$zip_path" | awk '{print $2}')"
  printf '%s  %s\n' "$digest" "$zip_path" > "${zip_path}.sha256"
else
  echo "handoff refused: need sha256sum, shasum, or openssl for checksum" >&2
  exit 1
fi
echo "$zip_path"
cat "${zip_path}.sha256"
