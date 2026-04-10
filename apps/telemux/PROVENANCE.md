# Provenance

`apps/telemux` was copied from `/home/swader/private-dev-factory` during the valkyrie productization pass.

The original upstream remote at migration time was `git@github.com:swader/clawdex.git` on branch `master`.

Product changes after copy:

- Runtime defaults now prefer `~/.local/state/brainstack` instead of `/srv`.
- Example worker paths are home-directory based.
- Telegram network/fetch errors redact bot-token-shaped strings before logging.

The copied app is first-class but optional. `single-node` and `control` profiles enable it by default; `worker` and `client-macos` do not run Telegram polling.

