# Plan: Add Detailed Error Messages to Cloud Backends

## TL;DR

Currently, all three cloud backends (S3, GCS, Azure) silently discard curl errors, HTTP status codes, and response bodies when `get_size` or `read_range` fails — returning only `-1`. Add an error message propagation mechanism so that failures surface detailed diagnostics (e.g., AWS XML error body with `InvalidAccessKeyId`) through to the R user.

## Steps

### Phase 1: Infrastructure — Error buffer and vtable extension

1. **`cloud_stream.h`**: Define `CLOUD_MAX_ERROR_LEN` (e.g., 4096). Add `char last_error[CLOUD_MAX_ERROR_LEN]` field to `CloudStream`. Add `const char *(*get_last_error)(void *backend_data)` function pointer to `CloudBackend` vtable. Declare `const char *cloud_stream_get_last_error(CloudStream *cs)` public API.

2. **Each backend struct**: Add `char last_error[CLOUD_MAX_ERROR_LEN]` to `S3BackendData`, `GCSBackendData`, `AzureBackendData`.

### Phase 2: Capture errors in backends

3. **`s3_backend.c`**: In `s3_get_size` and `s3_read_range`, when `curl_easy_perform` fails or HTTP status is not 200/206:
   - Capture response body via a `CurlBuffer` write callback (add to HEAD requests too)
   - Format error into `s3->last_error` with: provider prefix ("S3"), HTTP status code, curl error string (`curl_easy_strerror(res)`), and any response body (AWS returns XML error details for GET/PUT; HEAD may have empty body)
   - Example format: `"S3: HTTP 403 accessing 'https://...'. Server response: <?xml ...InvalidAccessKeyId...>"`
   - Implement `s3_get_last_error()` returning `s3->last_error`
   - Add to `s3_backend_vtable`

4. **`gcs_backend.c`**: Same pattern for `gcs_get_size` and `gcs_read_range`. Prefix "GCS". GCS returns JSON error bodies. Implement `gcs_get_last_error()`.

5. **`azure_backend.c`**: Same pattern for `azure_get_size` and `azure_read_range`. Prefix "Azure". Azure returns XML error bodies. Implement `azure_get_last_error()`.

*Steps 3–5 are parallel — independent backend files.*

### Phase 3: Propagate through cloud_stream

6. **`cloud_stream.c`**: In `cloud_stream_read`, `cloud_stream_seek`, and `cloud_stream_getsize`, after `get_size` or `read_range` returns `-1`:
   - If `cs->backend.get_last_error` is non-NULL, copy the backend's error string into `cs->last_error`
   - Implement `cloud_stream_get_last_error(CloudStream *cs)` returning `cs->last_error`

### Phase 4: Surface errors in R-facing layer

7. **`gdscloud.cpp`**: In `gdscloud_cb_read` and `gdscloud_cb_getsize`, when the underlying call returns `-1`, call `cloud_stream_get_last_error(cs)` and throw `ErrGDSCloud` with the detailed message. This happens before `GDS_File_Open_Callback` returns failure, so the R user sees the specific error.
   - Alternative: modify the `gdscloud_open_s3/gcs/azure` functions to pre-check `cloud_stream_getsize()` before calling `GDS_File_Open_Callback`, and throw with the detailed error on failure.

## Relevant Files

- `gdscloud/src/cloud_stream.h` — Add `CLOUD_MAX_ERROR_LEN`, `last_error` to `CloudStream`, `get_last_error` to `CloudBackend`, new public API
- `gdscloud/src/cloud_stream.c` — Implement `cloud_stream_get_last_error()`, propagate error after backend calls
- `gdscloud/src/s3_backend.c` — Capture curl + HTTP + body errors in `s3_get_size`/`s3_read_range`, implement `s3_get_last_error`
- `gdscloud/src/gcs_backend.c` — Same for GCS backend
- `gdscloud/src/azure_backend.c` — Same for Azure backend
- `gdscloud/src/gdscloud.cpp` — Throw `ErrGDSCloud` with detailed error from `cloud_stream_get_last_error()`

## Verification

1. Build the package: `R CMD build gdscloud && R CMD INSTALL gdscloud_*.tar.gz` — should compile with no warnings
2. Test with invalid AWS credentials → verify R error message includes HTTP status code and AWS XML error body (e.g., `InvalidAccessKeyId`)
3. Test with non-existent bucket/key → verify HTTP 404 with descriptive message
4. Test with valid credentials → verify normal operation is unaffected
5. Test with network-unreachable endpoint → verify curl error string appears

## Decisions

- Error buffer size: 4096 bytes — large enough for XML error bodies from cloud providers, small enough to not bloat memory
- `get_last_error` is a vtable function pointer (not a field) so existing backends without it can have it set to NULL for backward compatibility
- Response body capture: Add `CURLOPT_WRITEFUNCTION` to HEAD requests too (some servers return bodies on error HEAD); use a small temporary buffer
- For HEAD requests where body is empty, the error message still includes HTTP status code and curl error — this alone is very informative (e.g., "HTTP 403 Forbidden")
- `read_range` errors also capture details (not just `get_size`), since streaming errors can happen at any time
- Pre-check `get_size()` in `gdscloud_open_*` before `GDS_File_Open_Callback` — gives the user a clear error at open time rather than a generic "failed to open GDS file" from gdsfmt's callback layer
- Truncate response body portion at 1024 chars with `"... [truncated]"` suffix — some XML/JSON error responses from cloud providers can be very large
