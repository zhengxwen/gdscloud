## Plan: gdscloud — S3/GCS/Azure Read-Only GDS File Access

Add cloud storage (S3, GCS, Azure Blob) read-only access to GDS files. The approach: extend gdsfmt with a callback-based stream API, then implement cloud backends in gdscloud using libcurl with block caching.

---

### Phase 1: gdsfmt Extension — Callback Stream API

1. **Add `CdCallbackStream` class** — insert into existing `gdsfmt/src/CoreArray/dStream.h` and `dStream.cpp`. Derives from `CdStream`, wraps 4 function pointers (`read_fn`, `seek_fn`, `getsize_fn`, `close_fn` + `void *user_data`). Write methods throw errors (read-only).

2. **Add `GDS_File_Open_Callback()` C function** — in [R_CoreArray.cpp](gdsfmt/src/R_CoreArray.cpp). Creates a `CdCallbackStream`, calls `CdGDSFile::LoadStream()`, registers in `PKG_GDS_Files[]`. Signature: `PdGDSFile GDS_File_Open_Callback(read_fn, seek_fn, getsize_fn, close_fn, user_data, AllowError)`.

3. **Register as C-callable** — add `REG(GDS_File_Open_Callback)` in `R_init_gdsfmt()` at [R_CoreArray.cpp#L1920](gdsfmt/src/R_CoreArray.cpp#L1920).

4. **Update public headers** — add `extern` declaration in [R_GDS.h](gdsfmt/inst/include/R_GDS.h), add typedef + wrapper + LOAD in [R_GDS2.h](gdsfmt/inst/include/R_GDS2.h).

5. **URL scheme handler in R** — modify `openfn.gds()` in [gdsfmt-main.r](gdsfmt/R/gdsfmt-main.r) to detect `s3://`, `gs://`, `az://` prefixes and delegate to a registered handler. Add internal `.gds_register_cloud_handler()` / `.gds_get_cloud_handler()` stored in a package environment.

### Phase 2: gdscloud Package Scaffold

6. Create standard R package files: `DESCRIPTION` (Depends: gdsfmt, SystemRequirements: libcurl), `NAMESPACE`, `README.md`, `src/Makevars` (link libcurl).

### Phase 3: Common Cloud Stream Layer (in gdscloud)

7. **Common interface** — `gdscloud/src/cloud_stream.h` defines `CloudStream` struct with URL, position, block cache (1MB blocks, 64MB max, LRU eviction), and backend vtable (`read_range`, `get_size`, `close`).

8. **Block cache + common ops** — `gdscloud/src/cloud_stream.c` implements `cloud_stream_read()`, `cloud_stream_seek()`, `cloud_stream_getsize()`, `cloud_stream_close()` with LRU block cache.

9. **Callback adapter** — `gdscloud/src/gds_callbacks.c` bridges CloudStream to gdsfmt's callback function pointer interface.

### Phase 4: Cloud Backends (each in separate C file)

10. **S3 backend** — `gdscloud/src/s3_backend.c`. libcurl HTTP Range requests, AWS Signature V4 (HMAC-SHA256), credential reading from env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `AWS_SESSION_TOKEN`). URL parsing: `s3://bucket/key` → `https://bucket.s3.region.amazonaws.com/key`. *parallel with steps 11-12*

11. **GCS backend** — `gdscloud/src/gcs_backend.c`. libcurl, OAuth2/service account auth, `gs://bucket/key` → `https://storage.googleapis.com/bucket/key`. *parallel with steps 10, 12*

12. **Azure backend** — `gdscloud/src/azure_backend.c`. libcurl, Shared Key/SAS token auth, `az://container/blob` → `https://account.blob.core.windows.net/container/blob`. *parallel with steps 10-11*

### Phase 5: gdscloud R Implementation

13. **Cloud open function** — `gdscloud/R/cloud_open.r` with `gdsCloudOpen(url, ...)`. Parses URL scheme, calls appropriate C entry point, returns `gds.class` object.

14. **Credential configuration** — `gdscloud/R/credentials.r`: `gdsCloudConfigS3()` for AWS, `gdsCloudConfigGCS()` for GCS, `gdsCloudConfigAzure()` for Azure. Falls back to env vars.

15. **Cache control** — `gdscloud/R/cache.r`: `gdsCloudCacheSize()`, `gdsCloudCacheClear()`, `gdsCloudInfo()`.

16. **Handler registration** — `gdscloud/R/zzz.r` `.onLoad()` calls `gdsfmt:::.gds_register_cloud_handler("s3", ...)` etc., so `openfn.gds("s3://...")` works transparently.

### Phase 6: C-R Bridge in gdscloud

17. **C entry points** — `gdscloud/src/gdscloud.c` with `.Call` functions: `gdscloud_open_s3(url, key, secret, region, session_token, cache_size)`, `gdscloud_open_gcs(...)`, `gdscloud_open_azure(...)`. Each creates CloudStream, calls `GDS_File_Open_Callback()` via `R_GetCCallable`. *depends on Phase 1 and Phase 3-4*

18. **Package init** — `gdscloud/src/gdscloud_init.c`: `R_init_gdscloud()` registers `.Call` methods, calls `Init_GDS_Routines()`.

---

### Relevant Files

**gdsfmt (modified)**:
- [gdsfmt/src/CoreArray/dStream.h](gdsfmt/src/CoreArray/dStream.h) — add CdCallbackStream class definition
- [gdsfmt/src/CoreArray/dStream.cpp](gdsfmt/src/CoreArray/dStream.cpp) — add CdCallbackStream implementation
- [gdsfmt/src/R_CoreArray.cpp](gdsfmt/src/R_CoreArray.cpp) — add ~25 lines (GDS_File_Open_Callback + REG)
- [gdsfmt/inst/include/R_GDS.h](gdsfmt/inst/include/R_GDS.h) — add ~3 lines
- [gdsfmt/inst/include/R_GDS2.h](gdsfmt/inst/include/R_GDS2.h) — add ~15 lines
- [gdsfmt/R/gdsfmt-main.r](gdsfmt/R/gdsfmt-main.r) — modify `openfn.gds()`, add ~30 lines

**gdscloud (new)**:
- `gdscloud/DESCRIPTION`, `NAMESPACE`, `README.md`
- `gdscloud/src/Makevars`, `Makevars.win`
- `gdscloud/src/cloud_stream.h`, `cloud_stream.c` — common interface + cache
- `gdscloud/src/gds_callbacks.c` — gdsfmt callback adapter
- `gdscloud/src/s3_backend.c`, `gcs_backend.c`, `azure_backend.c`
- `gdscloud/src/gdscloud.c`, `gdscloud_init.c` — R-.Call bridge
- `gdscloud/R/cloud_open.r`, `credentials.r`, `cache.r`, `zzz.r`

### Verification

1. `R CMD build gdsfmt && R CMD check` — no regressions, callback stream compiles
2. `R CMD build gdscloud && R CMD check` — package builds with libcurl
3. Open a local GDS file through the callback API to verify correctness
4. Test S3 access with a known bucket/file, verify data matches local copy
5. Verify repeated reads to same region don't trigger extra HTTP requests (cache works)
6. All existing gdsfmt tests pass unchanged

### Decisions

- **Read-only only**: write operations throw errors
- **Callback API over C++ inheritance**: avoids downstream packages needing CoreArray headers
- **URL scheme detection in `openfn.gds()`**: transparent UX when gdscloud is loaded
- **libcurl at C level**: maximum performance for HTTP I/O
- **Block cache**: 1MB blocks, 64MB default max, LRU eviction
- **Multi-cloud**: S3 + GCS + Azure in separate C files sharing a common `CloudStream` interface
