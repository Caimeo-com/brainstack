# Provenance

`apps/braind/src/server.ts` was copied from `/home/swader/shared-brain/app/src/server.ts` during the valkyrie productization pass.

`apps/braind/src/brain-lib.ts` was copied from `/home/swader/shared-brain/live/shared-brain/tools/lib/brain.ts`.

Product changes after copy:

- Server imports the library from product-owned source instead of the live content repo.
- Default repo root prefers `~/shared-brain/serve/shared-brain` and falls back to the legacy valkyrie live checkout.
- Large binary imports can be stored in a content-addressed blob store outside git while manifests and normalized extracts remain in git.

