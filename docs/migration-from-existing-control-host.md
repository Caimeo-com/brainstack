# Migration From Existing Control Host

An existing control host is intentionally not switched automatically by creating `~/brainstack`.

## Current Production Inputs

- Shared brain app source: `~/shared-brain/app`
- Shared brain canonical live checkout: `~/shared-brain/live/shared-brain`
- Shared brain bare repo: `~/shared-brain/bare/shared-brain.git`
- Legacy telemux source: `<legacy-telemux-source>`
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

After the remodel, production `shared-brain.service` should use `<home>/brainstack/apps/braind/src/server.ts`, read from `<home>/shared-brain/serve/shared-brain`, and write through `<home>/shared-brain/staging/shared-brain`.

## Compatibility Plan

On the control host, preserve the current service until a deliberate cutover:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts migrate-current-install
```

This writes a compatibility config at:

```text
~/.config/brainstack/current-control.brainstack.yaml
```

It does not stop services, move repos, rewrite hooks, or delete `/srv`.

## Safe Cutover Outline

1. Confirm backups exist under `~/.local/state/brainstack/backups`.
2. Run `smoke` for `single-node` and `control`.
3. Create staging and serve clones from the existing bare repo.
4. Point `braind.env` at serve and staging clones.
5. Replace the old post-receive hook with the brainstack hook that updates only the serve clone.
6. Restart `braind`.
7. Test `/healthz`, admin `/admin/health`, page rendering, search, import, ingest, and git push.
8. Only then retire the old `~/shared-brain/app` compatibility source.

## Current Known Legacy Issues

- Review historical service logs for sensitive values according to local retention policy. Rotate any potentially exposed bot token or API token before relying on a migrated service.
- Control-to-worker `tcp:22` may require disabling Tailscale SSH on the worker, enabling normal `sshd.service`, installing a dedicated control-to-worker SSH key, and setting the worker's `sshUser` explicitly.
- Worker hosts should advertise `tag:brain-worker`; control hosts should request `tag:brain`. Validate server-side tag application with `tailscale status` plus `tailscale whois <control-tailscale-ip>` before removing any temporary host/IP fallback grants from the live tailnet policy.
- Current production telemux still uses `/srv/telemux` and `/srv/factory`; brainstack defaults are home-directory based for future installs.
