# Migration From Current Valkyrie

Current valkyrie is intentionally not switched automatically by creating `~/brainstack`.

## Current Production Inputs

- Shared brain app source: `~/shared-brain/app`
- Shared brain canonical live checkout: `~/shared-brain/live/shared-brain`
- Shared brain bare repo: `~/shared-brain/bare/shared-brain.git`
- Clawdex/private-dev-factory source: `~/private-dev-factory`
- Telemux state: `/srv/telemux`
- Factory workspaces: `/srv/factory`
- System service: `shared-brain.service`
- User service: `telemux.service`

## Product Target

Fresh installs use:

- `~/shared-brain/bare/shared-brain.git`
- `~/shared-brain/staging/shared-brain`
- `~/shared-brain/serve/shared-brain`

The web server reads from the serve clone. Write APIs use the staging clone and push to the bare repo. The bare repo post-receive hook updates the serve clone and reindexes. This removes the dangerous old behavior where a hook could hard-reset a checkout humans were editing.

As of the valkyrie remodel, production `shared-brain.service` uses `/home/swader/brainstack/apps/braind/src/server.ts`, reads from `/home/swader/shared-brain/serve/shared-brain`, and writes through `/home/swader/shared-brain/staging/shared-brain`.

## Compatibility Plan

On valkyrie, preserve the current service until a deliberate cutover:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts migrate-current-install
```

This writes a compatibility config at:

```text
~/.config/brainstack/valkyrie-current.brainstack.yaml
```

It does not stop services, move repos, rewrite hooks, or delete `/srv`.

## Safe Cutover Outline

1. Confirm backups exist under `~/.local/state/brainstack/backups`.
2. Run `smoke` for `single-node` and `control`.
3. Create staging and serve clones from the existing bare repo.
4. Point `braind.env` at serve and staging clones.
5. Replace the old post-receive hook with the brainstack hook that updates only the serve clone.
6. Restart `braind`.
7. Test `/health`, page rendering, search, import, ingest, and git push.
8. Only then retire the old `~/shared-brain/app` compatibility source.

## Current Known Valkyrie Issues

- Historical journald logs contained leaked Telegram bot-token material. The customer-zero token was rotated and telemux was restarted successfully; future operators should still rotate any leaked bot token in BotFather and sanitize logs according to local retention policy.
- `valkyrie -> erbine tcp:22` was unblocked during customer-zero setup by disabling Tailscale SSH on erbine, enabling normal `sshd.service`, installing a dedicated `valkyrie_to_erbine_ed25519` key for `swader@erbine`, and changing current telemux worker config to `sshUser: swader`.
- Erbine now advertises `tag:brain-worker`. Valkyrie locally requests `tag:brain`; validate server-side application with `tailscale status` plus `tailscale whois <valkyrie-tailscale-ip>` before removing any temporary host/IP fallback grants from the live tailnet policy.
- Current production telemux still uses `/srv/telemux` and `/srv/factory`; brainstack defaults are home-directory based for future installs.
