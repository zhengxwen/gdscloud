## Plan: Fix Deprecated OpenSSL API for Portability

Replace deprecated OpenSSL legacy SHA256 and HMAC APIs with version-conditional code. Uses EVP APIs on OpenSSL 3.0+ and falls back to legacy APIs on older versions. Two files are affected.

---

**Steps**

### Phase 1: s3_backend.c (3 changes)

1. **Update includes** (L24–25): Replace `#include <openssl/sha.h>` with `#include <openssl/evp.h>` and add `#include <openssl/opensslv.h>` for version detection.

2. **Replace `sha256_hash()`** (L89–97): Use `EVP_Digest()` unconditionally — it's a one-shot function available since OpenSSL 0.9.7 and **not** deprecated in 3.0. No `#if` needed:
   - `EVP_Digest(data, len, out, &md_len, EVP_sha256(), NULL)`

3. **Replace `hmac_sha256()`** (L103–108): Version-conditional `#if OPENSSL_VERSION_NUMBER >= 0x30000000L`:
   - **OpenSSL 3.0+**: Use `EVP_MAC` API (`EVP_MAC_fetch` → `EVP_MAC_CTX_new` → `OSSL_PARAM_construct_utf8_string("digest","SHA256")` → `EVP_MAC_init` → `EVP_MAC_update` → `EVP_MAC_final` → cleanup)
   - **OpenSSL < 3.0**: Keep existing `HMAC(EVP_sha256(), ...)` call

### Phase 2: azure_backend.c *(parallel with Phase 1)*

4. **Update includes** (L23–28): Add `#include <openssl/opensslv.h>`. Remove `#include <openssl/sha.h>`.

5. **Replace `HMAC()` call** (L183–185): Same version-conditional pattern as step 3 — `EVP_MAC` on 3.0+, legacy `HMAC()` on older.

### Phase 3: Optional shared header

6. Extract a shared `openssl_compat.h` with portable `sha256_hash()` and `hmac_sha256()` helpers so both backends reuse the same code instead of duplicating the `#if` logic.

---

**Relevant files**
- `gdscloud/src/s3_backend.c` — `sha256_hash()` (L89–97), `hmac_sha256()` (L103–108), includes (L24–25)
- `gdscloud/src/azure_backend.c` — `HMAC()` at L183, includes (L23–28)
- `gdscloud/src/Makevars` / `Makevars.win` — no changes needed (already links `-lcrypto`)

**Verification**
1. `R CMD INSTALL gdscloud` on macOS with Homebrew OpenSSL 3.x — confirm **zero** deprecation warnings
2. If possible, build on a system with OpenSSL 1.1.x to confirm the `#else` branch compiles
3. `R CMD check gdscloud` — full package check passes

**Decisions**
- `EVP_Digest()` for SHA256 needs **no** `#if` — it's portable across all OpenSSL versions
- `HMAC()` for HMAC-SHA256 **does** need `#if` — the replacement `EVP_MAC` API only exists in 3.0+
- `#include <openssl/sha.h>` can be dropped from both files
- Shared helper header (step 6) is optional — keeps things DRY but adds a file
