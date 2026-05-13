// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// openssl_compat.h: Portable OpenSSL helpers for SHA-256 and HMAC-SHA256
//     Works with both OpenSSL 1.x (legacy API) and OpenSSL 3.x (EVP API)
//
// Copyright (C) 2026    Xiuwen Zheng
//
// This file is part of gdscloud.
// GPL-3 License
// ===========================================================

#ifndef GDSCLOUD_OPENSSL_COMPAT_H
#define GDSCLOUD_OPENSSL_COMPAT_H

#include <openssl/opensslv.h>
#include <openssl/evp.h>

#if OPENSSL_VERSION_NUMBER >= 0x30000000L
#include <openssl/params.h>
#include <openssl/core_names.h>
#else
#include <openssl/hmac.h>
#endif

#include <string.h>


// =====================================================================
// Portable SHA-256 hash (one-shot)
// EVP_Digest() is available since OpenSSL 0.9.7 and NOT deprecated in 3.0
// =====================================================================

static inline void sha256_hash(const unsigned char *data, size_t len,
	unsigned char *out)
{
	unsigned int md_len = 32;
	EVP_Digest(data, len, out, &md_len, EVP_sha256(), NULL);
}


// =====================================================================
// Portable HMAC-SHA256
// OpenSSL 3.0+: EVP_MAC API
// OpenSSL < 3.0: legacy HMAC() one-shot
// =====================================================================

static inline void hmac_sha256(const unsigned char *key, size_t key_len,
	const unsigned char *data, size_t data_len, unsigned char *out)
{
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
	EVP_MAC *mac = EVP_MAC_fetch(NULL, "HMAC", NULL);
	EVP_MAC_CTX *ctx = EVP_MAC_CTX_new(mac);
	OSSL_PARAM params[2];
	params[0] = OSSL_PARAM_construct_utf8_string(
		OSSL_MAC_PARAM_DIGEST, "SHA256", 0);
	params[1] = OSSL_PARAM_construct_end();
	EVP_MAC_init(ctx, key, key_len, params);
	EVP_MAC_update(ctx, data, data_len);
	size_t out_len = 32;
	EVP_MAC_final(ctx, out, &out_len, 32);
	EVP_MAC_CTX_free(ctx);
	EVP_MAC_free(mac);
#else
	unsigned int len = 32;
	HMAC(EVP_sha256(), key, (int)key_len, data, data_len, out, &len);
#endif
}

#endif /* GDSCLOUD_OPENSSL_COMPAT_H */
