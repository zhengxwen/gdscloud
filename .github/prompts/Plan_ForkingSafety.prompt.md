# Plan: Fork Safety via getpid() Detection in gdscloud

Add lazy CURL handle reinitialization in forked child processes by storing `pid_t` at creation time and comparing with `getpid()` before each CURL operation. When a mismatch is detected (child process), destroy the inherited handle, create a fresh one, and clear the cache. This makes gdscloud safe with `parallel::mclapply()` on Unix.

---

**Steps**

### Phase 1: Fork tracking infrastructure
1. In `cloud_stream.h` — add `#include <unistd.h>` (guarded `#ifndef _WIN32`), add `pid_t creator_pid` field to `CloudStream` struct, declare two new functions: `cloud_init_fork_tracking()` and `cloud_check_reinit_curl(CURL **curl_ptr)`
2. In `cloud_stream.c` — add a `static pid_t g_curl_init_pid` global. Implement:
   - `cloud_init_fork_tracking()` — stores `getpid()` into `g_curl_init_pid`
   - `cloud_check_reinit_curl(CURL **curl_ptr)` — compares `getpid()` vs `g_curl_init_pid`; if different, re-calls `curl_global_init(CURL_GLOBAL_ALL)`, calls `curl_easy_cleanup(*curl_ptr)` + `*curl_ptr = curl_easy_init()`, updates `g_curl_init_pid`, returns 1; otherwise returns 0

### Phase 2: Record pid at package init
3. In `gdscloud_init.c` — call `cloud_init_fork_tracking()` after `curl_global_init(CURL_GLOBAL_ALL)` in `R_init_gdscloud()`

### Phase 3: Add fork checks to each backend (*steps 4-6 are parallel*)
4. In `s3_backend.c` — add `cloud_check_reinit_curl(&s3->curl)` at the top of `s3_read_range()` and `s3_get_size()`, before any `curl_easy_*` usage
5. In `gcs_backend.c` — same pattern in `gcs_read_range()` and `gcs_get_size()`
6. In `azure_backend.c` — same pattern in `azure_read_range()` and `azure_get_size()`

### Phase 4: Cache invalidation on fork
7. In `cloud_stream_create()` — store `getpid()` into `cs->creator_pid`
8. In `cloud_stream_read()` — before cache lookup, check `getpid() != cs->creator_pid`; if so, call `cache_clear_all(&cs->cache)`, reset `cs->file_size = -1`, update `cs->creator_pid`. This ensures the child re-fetches data through its own fresh CURL handle

---

**Relevant files**
- `gdscloud/src/cloud_stream.h` — add `pid_t creator_pid` to `CloudStream`, declare fork-tracking functions
- `gdscloud/src/cloud_stream.c` — implement `g_curl_init_pid`, `cloud_init_fork_tracking()`, `cloud_check_reinit_curl()`, pid check in `cloud_stream_create()` and `cloud_stream_read()`
- `gdscloud/src/gdscloud_init.c` — call `cloud_init_fork_tracking()` in `R_init_gdscloud()`
- `gdscloud/src/s3_backend.c` — add `cloud_check_reinit_curl()` call in `s3_read_range()` and `s3_get_size()`
- `gdscloud/src/gcs_backend.c` — same in `gcs_read_range()` and `gcs_get_size()`
- `gdscloud/src/azure_backend.c` — same in `azure_read_range()` and `azure_get_size()`

---

**Verification**
1. `R CMD INSTALL gdscloud` — must compile without warnings
2. Single-process smoke test: open a cloud GDS file, read data, confirm still works
3. Fork test: `parallel::mclapply(1:4, function(i) { ... read from cloud GDS ... }, mc.cores=4)` — must not crash or deadlock
4. Add temporary `Rprintf("fork detected, reinitializing curl\n")` in `cloud_check_reinit_curl()` to confirm it fires in mclapply children

---

**Decisions**
- **No `pthread_atfork()`** — `getpid()` lazy detection is simpler and works even if the package is loaded after fork setup
- **Cache invalidated in child** — child clears inherited cache and re-fetches. Minimal cost since it refills on demand
- **`curl_global_init()` re-called in child** — safe per libcurl docs (reference-counted). We do NOT call `curl_global_cleanup()` in the child (unsafe after fork)
- **`curl_easy_cleanup()` on inherited handle** — safe because handle is always idle between operations (`curl_easy_reset()` is called at the start of each request)
- **Windows guarded** — `#ifndef _WIN32` around pid tracking; Windows has no `fork()`
