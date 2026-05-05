## Plan: URL-specific cloud credentials

**TL;DR** — Extend the existing `gdsCloudConfigS3/GCS/Azure` functions with an optional `url=` argument so users can register per-URL credential sets. Matching uses longest-prefix (within the same scheme). When opening a URL, the backend resolves credentials as `URL entry → global → env var`, with only non-`NULL`/non-empty values overriding lower layers. `gdsCloudExportCredentials()` ships the URL table to workers too.

**Steps**

1. **Init storage** — in [R/zzz.r](R/zzz.r) `.onLoad()`, add `.gdscloud_env$url_credentials <- list()`.
2. **Helpers** (internal, in [R/credentials.r](R/credentials.r)) — `.normalize_url_prefix()`, `.set_url_credentials()`, `.match_url_credentials()`, and a small merge helper. *Parallel with step 3.*
3. **Extend `gdsCloudConfigS3/GCS/Azure`** with `url=NULL`. When `url` is `NULL`, keep current behavior (set globals). When `url` is supplied, validate scheme match and store the non-`NULL` fields in `url_credentials`; if all fields are `NULL`, remove the entry. *Parallel with step 2.*
4. **Update `.get_s3/gcs/azure_credentials()`** to take `url=NULL` and layer env → global → URL-specific, with `region` default preserved. *Depends on steps 2–3.*
5. **Thread URL into backends** — `.open_s3/.open_gcs/.open_azure` in [R/cloud_open.r](R/cloud_open.r) call the getters with the opened `url`. *Depends on step 4.*
6. **Worker propagation** — extend `.collect_credentials()` and `.install_credentials()` in [R/credentials.r](R/credentials.r) to include/restore `url_credentials`. *Parallel with step 5.*
7. **Docs** — update [man/gdsCloudConfigS3.Rd](man/gdsCloudConfigS3.Rd): add `url` to usage/args, document matching and merge rules, add a `\dontrun` example with two buckets. *Parallel with code.*
8. **Tests** — extend [tests/testthat/test-credentials.R](tests/testthat/test-credentials.R): URL entry stored without clobbering globals; all-`NULL` with `url` removes the entry; per-URL vs global resolution; longest-prefix wins; scheme mismatch errors; cluster export carries `url_credentials`. *Depends on steps 1–6.*

**Relevant files**
- [R/credentials.r](R/credentials.r) — extend config functions, rework `.get_*_credentials`, add helpers, extend collect/install.
- [R/cloud_open.r](R/cloud_open.r) — pass `url` into the getters.
- [R/zzz.r](R/zzz.r) — initialize `url_credentials`.
- [man/gdsCloudConfigS3.Rd](man/gdsCloudConfigS3.Rd) — document `url` and matching rules.
- [tests/testthat/test-credentials.R](tests/testthat/test-credentials.R) — new tests.
- [NAMESPACE](NAMESPACE) — no changes (no new exports).

**Verification**
1. `R CMD build .` then `R CMD check --no-manual` — clean (Rd still valid).
2. `devtools::test()` — all existing plus new tests pass.
3. Manual: set a global S3 key, then register a URL-specific one; verify `gdscloud:::.get_s3_credentials("s3://bucketA/x.gds")` returns the per-URL key while `gdscloud:::.get_s3_credentials("s3://other/x.gds")` returns the global key.
4. Cluster: after `gdsCloudExportCredentials(cl)`, `clusterEvalQ(cl, gdscloud:::.gdscloud_env$url_credentials)` matches the parent.

**Decisions**
- **Matching**: longest-prefix within the same scheme; keys normalized with a trailing `/`.
- **API**: reuse existing `gdsCloudConfig*` with new `url=` arg (vs. a new function).
- **Fallback**: merge URL > global > env; only non-empty values override.
- **Removal**: `gdsCloudConfigS3(url="s3://b/")` with no other args deletes that entry.
- **Export**: URL-specific creds are forwarded to workers.
- **Out of scope**: regex/glob matching, on-disk credential files (e.g. `~/.aws/credentials`), AWS profile parsing.
