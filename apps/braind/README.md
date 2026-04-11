# braind

`braind` is the shared-brain web/API/search/import service.

Runtime defaults:

- Bind: `127.0.0.1`
- Port: `8080`
- Serve clone: `~/shared-brain/serve/shared-brain`
- Blob store: `~/.local/state/brainstack/blobs/shared-brain`
- Large binary threshold: `10 MiB`

The service writes canonical changes through a git checkout supplied by `SHARED_BRAIN_REPO_ROOT`. For fresh `brainstack` installs this is the staging clone for write operations and the serve clone for read serving; the generated hook updates the serve clone after pushes.

Legacy compatibility can point `SHARED_BRAIN_REPO_ROOT` at `~/shared-brain/live/shared-brain` until migration is intentionally applied.
