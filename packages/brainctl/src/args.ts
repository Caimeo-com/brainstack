export interface ParsedArgs {
  command: string;
  positional: string[];
  flags: Record<string, string | boolean | string[]>;
}

export function usage(): string {
  return `Usage:
  brainctl init --profile single-node|control|worker|client-macos --config brainstack.yaml [--dry-run] [--root /tmp/install-root] [--seed-missing|--force-seed] [--import-token-file FILE]
  brainctl provision --profile single-node|control|worker|client-macos [--out brainstack.yaml] [--harness codex|claude] [--harness-bin PATH_OR_NAME] [--enable-telemux] [--enroll-tailscale] [--tailscale-tag tag:brain] [--brain-base-url URL] [--brain-remote SSH_OR_PATH] [--require-harness-sudo] [--test-bot]
  brainctl upgrade --config brainstack.yaml [--profile ...] [--dry-run] [--root /tmp/install-root]
  brainctl apply-runtime --config brainstack.yaml [--profile ...] [--dry-run] [--root /tmp/install-root]
  brainctl doctor --config brainstack.yaml [--profile ...] [--json] [--workers] [--deep] [--write-smoke]
  brainctl status --config brainstack.yaml [--json] [--timeout-ms N]
  brainctl lifecycle status|repair|upgrade|uninstall --config brainstack.yaml [--target codex|claude|cursor|all] [--dry-run]
  brainctl daemon run|once|status|install|uninstall|logs --config brainstack.yaml [--json]
  brainctl updates --config brainstack.yaml [--profile ...]
  brainctl fleet status|update --config brainstack.yaml [machine|--all] [--json] [--dry-run]
  brainctl capabilities list|install|doctor|test [voice] --config brainstack.yaml [--target MACHINE] [--model tiny.en|small.en|medium.en|large-v2|large-v3]
  brainctl expose tailscale --config brainstack.yaml --dry-run|--apply
  brainctl backup --config brainstack.yaml [--out DIR] [--pause-telemux]
  brainctl restore --backup DIR_OR_TGZ --target DIR [--apply]
  brainctl render --config brainstack.yaml --profile ... --out DIR
  brainctl bootstrap-client --profile client-macos --config brainstack.yaml --out DIR
  brainctl skills install [--target codex] [--profile client|operator|control|worker] [--skill NAME|--all] [--dir DIR] [--dry-run]
  brainctl skills refresh [--target codex|claude|cursor] [--config brainstack.yaml] [--repo PATH] [--skill NAME] [--dir DIR] [--no-sync] [--force] [--quiet]
  brainctl skills doctor [--target codex|claude|cursor] [--dir DIR] [--check-remote] [--json]
  brainctl skills list
  brainctl import skill <SKILL.md|DIR|URL> --config brainstack.yaml [--title TITLE] [--source-harness HARNESS] [--source-machine MACHINE] [--max-bytes N] [--max-files N] [--allow-private-url] [--allow-ssh-git]
  brainctl import skills [--config brainstack.yaml] [--target codex|claude|cursor|all] [--scan-dir DIR] [--skill NAME] [--apply] [--json]
  brainctl import codex-session <SESSION_ID|JSONL_PATH> [--config brainstack.yaml] [--include-transcript] [--max-bytes N] [--dry-run] [--json]
  brainctl hooks install|status|remove [--target codex|claude|cursor|all] [--config brainstack.yaml] [--brainctl PATH] [--brainctl-command COMMAND] [--dry-run]
  brainctl hook run --harness codex|claude|cursor --event EVENT [--config brainstack.yaml]
  brainctl invite create --config brainstack.yaml [--import-token-file FILE|--import-token-env ENV] [--ssh-known-hosts-file FILE] [--control-ssh SSH_TARGET] [--control-repo PATH] [--skills-profile client|operator|control|worker|none] [--expires-hours N] [--install-version TAG|latest|--install-url URL] [--allow-insecure-install-url] [--json]
  brainctl enroll --invite bs1_...|--invite-file FILE|- [--config brainstack.yaml] [--skills-profile client|operator|control|worker|none] [--skip-skills] [--skip-init] [--skip-doctor] [--force]
  brainctl join-worker --config brainstack.yaml --worker WORKER_HOST [--ssh-user USER] [--out DIR]
  brainctl trust-worker --config brainstack.yaml --worker WORKER_NAME [--host HOST] [--dry-run]
  brainctl worker-cache status|clear --config brainstack.yaml [worker|--all]
  brainctl repo-lock status|clear --config brainstack.yaml [--repo write|serve] [--path LOCK_DIR] [--yes] [--token LOCK_TOKEN] [--force] [--min-age-ms MS]
  brainctl locks status|clear --config brainstack.yaml [--repo write|serve] [--path LOCK_DIR] [--yes] [--token LOCK_TOKEN] [--force] [--min-age-ms MS]
  brainctl rotate-token --kind import|admin --config brainstack.yaml [--env FILE]
  brainctl telegram send-file --file PATH [--config brainstack.yaml] [--via SSH_TARGET] [--remote-repo PATH] [--caption TEXT] [--context SLUG|--chat-id ID] [--thread-id ID] [--kind document|photo] [--max-bytes N] [--allow-sensitive] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default] [--json]
  brainctl import-text --config brainstack.yaml --title TITLE --text TEXT --source-harness HARNESS --source-machine MACHINE [--source-type note]
  brainctl propose --config brainstack.yaml --title TITLE --body BODY [--target-page wiki/PATH.md] [--content-file FILE] [--base-sha256 HASH|absent] [--risk low|medium|high] [--confidence 0..1] [--curator-run-id ID] [--reason TEXT] [--needs-human] [--source-ids id1,id2] [--project NAME] [--domain NAME] [--scope repo|project|global|machine|harness] [--memory-kind KIND] [--applicability TEXT] [--non-applicability TEXT] [--evidence REF]
  brainctl proposals list|groups|clusters|show|enrich|reprocess|merge-group|approve|reject|apply [...]
  brainctl curator status|run|install [--config brainstack.yaml]
  brainctl context --repo PATH [--config brainstack.yaml] [--json] [--sync|--no-sync]
  brainctl search --repo PATH "query" [--config brainstack.yaml] [--json] [--wait-fresh]
  brainctl remember --repo PATH --summary TEXT [--target BRAIN_ID] [--confirm-cross-brain] [--project NAME] [--domain NAME] [--scope repo|project|global|machine|harness] [--memory-kind KIND] [--applicability TEXT] [--non-applicability TEXT] [--evidence REF]
  brainctl allow repo --repo PATH --brain BRAIN_ID [--sections a,b] --always|--once|--deny
  brainctl outbox status|list|flush|retry|purge|purge-corrupt --config brainstack.yaml [ID|--all] [--yes]
  brainctl destroy --config brainstack.yaml [--profile ...] --dry-run|--yes [--scope control|worker|client|all] [--remove-shared-brain] [--remove-private-brain] [--remove-tailscale-serve]
  brainctl migrate-current-install [--out FILE]
  brainctl smoke --profile single-node|control|worker|client-macos --config brainstack.yaml`;
}

export function parseArgs(argv: string[]): ParsedArgs {
  const [command = "help", ...rest] = argv;
  const positional: string[] = [];
  const flags: Record<string, string | boolean | string[]> = {};
  for (let index = 0; index < rest.length; index += 1) {
    const item = rest[index];
    if (!item.startsWith("--")) {
      positional.push(item);
      continue;
    }
    const equalsIndex = item.indexOf("=");
    if (equalsIndex > 2) {
      const key = item.slice(2, equalsIndex);
      const value = item.slice(equalsIndex + 1);
      if (flags[key] === undefined) {
        flags[key] = value;
      } else if (Array.isArray(flags[key])) {
        (flags[key] as string[]).push(value);
      } else {
        flags[key] = [String(flags[key]), value];
      }
      continue;
    }
    const key = item.slice(2);
    const next = rest[index + 1];
    if (!next || next.startsWith("--")) {
      flags[key] = true;
      continue;
    }
    if (flags[key] === undefined) {
      flags[key] = next;
    } else if (Array.isArray(flags[key])) {
      (flags[key] as string[]).push(next);
    } else {
      flags[key] = [String(flags[key]), next];
    }
    index += 1;
  }
  return { command, positional, flags };
}

export function flag(args: ParsedArgs, key: string): string | undefined {
  const value = args.flags[key];
  if (Array.isArray(value)) {
    return value[value.length - 1];
  }
  return typeof value === "string" ? value : undefined;
}

export function flagValues(args: ParsedArgs, key: string): string[] {
  const value = args.flags[key];
  if (value === true) {
    throw new Error(`--${key} requires a value`);
  }
  if (value === undefined) {
    return [];
  }
  return (Array.isArray(value) ? value : [value]).flatMap((entry) => entry.split(",").map((item) => item.trim()).filter(Boolean));
}

export function hasFlag(args: ParsedArgs, key: string): boolean {
  return args.flags[key] === true;
}

export function boolFlag(args: ParsedArgs, key: string, fallback = false): boolean {
  if (args.flags[key] === true) {
    return true;
  }
  const value = flag(args, key);
  if (value === undefined) {
    return fallback;
  }
  return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

export function requireFlagValue(args: ParsedArgs, key: string): string | undefined {
  if (args.flags[key] === true) {
    throw new Error(`--${key} requires a value`);
  }
  return flag(args, key);
}

export function commandHasHelp(args: ParsedArgs): boolean {
  const first = args.positional[0];
  return hasFlag(args, "help") || first === "help" || first === "--help" || first === "-h";
}
