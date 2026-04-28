// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// s3_backend.c: Amazon S3 backend using libcurl + AWS Signature V4
//
// Copyright (C) 2026    Xiuwen Zheng
//
// This file is part of gdscloud.
// LGPL-3 License
// ===========================================================

#include "cloud_stream.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <ctype.h>
#include <curl/curl.h>

#include <R.h>
#include <Rinternals.h>

// OpenSSL for HMAC-SHA256
#include <openssl/hmac.h>
#include <openssl/sha.h>


// =====================================================================
// S3 backend state
// =====================================================================

typedef struct S3BackendData {
    char access_key[CLOUD_MAX_CRED_LEN];
    char secret_key[CLOUD_MAX_CRED_LEN];
    char session_token[CLOUD_MAX_CRED_LEN];
    char region[128];
    char bucket[512];
    char object_key[CLOUD_MAX_URL_LEN];
    char endpoint[CLOUD_MAX_URL_LEN];  // resolved HTTPS endpoint
    CURL *curl;
} S3BackendData;


// =====================================================================
// Helper: curl write callback to buffer
// =====================================================================

typedef struct {
    unsigned char *buf;
    long long size;
    long long capacity;
} CurlBuffer;

static size_t curl_write_cb(void *data, size_t size, size_t nmemb, void *userp)
{
    CurlBuffer *cb = (CurlBuffer *)userp;
    size_t realsize = size * nmemb;
    if (cb->size + (long long)realsize > cb->capacity)
        realsize = (size_t)(cb->capacity - cb->size);
    if (realsize > 0)
    {
        memcpy(cb->buf + cb->size, data, realsize);
        cb->size += realsize;
    }
    return size * nmemb;  // always consume all to avoid curl error
}


// =====================================================================
// Helper: hex encoding
// =====================================================================

static void hex_encode(const unsigned char *in, size_t len, char *out)
{
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++)
    {
        out[i*2]   = hex[(in[i] >> 4) & 0x0f];
        out[i*2+1] = hex[in[i] & 0x0f];
    }
    out[len*2] = '\0';
}


// =====================================================================
// Helper: SHA-256 hash
// =====================================================================

static void sha256_hash(const unsigned char *data, size_t len,
    unsigned char *out)
{
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(out, &ctx);
}


// =====================================================================
// Helper: HMAC-SHA256
// =====================================================================

static void hmac_sha256(const unsigned char *key, size_t key_len,
    const unsigned char *data, size_t data_len, unsigned char *out)
{
    unsigned int len = 32;
    HMAC(EVP_sha256(), key, (int)key_len, data, data_len, out, &len);
}


// =====================================================================
// AWS Signature V4
// =====================================================================

static void aws_sigv4_sign(S3BackendData *s3, const char *method,
    const char *uri_path, const char *query_string,
    const char *payload_hash, const char *range_header,
    long long range_start, long long range_end,
    char *auth_header, size_t auth_size,
    char *date_header, size_t date_size,
    char *token_header, size_t token_size)
{
    time_t now = time(NULL);
    struct tm utc;
#ifdef _WIN32
    gmtime_s(&utc, &now);
#else
    gmtime_r(&now, &utc);
#endif

    char datestamp[16], amzdate[32];
    strftime(datestamp, sizeof(datestamp), "%Y%m%d", &utc);
    strftime(amzdate, sizeof(amzdate), "%Y%m%dT%H%M%SZ", &utc);

    // date header
    snprintf(date_header, date_size, "x-amz-date: %s", amzdate);

    // token header
    token_header[0] = '\0';
    if (s3->session_token[0])
        snprintf(token_header, token_size,
            "x-amz-security-token: %s", s3->session_token);

    // canonical headers
    char canonical_headers[2048];
    char signed_headers[512];

    if (range_header && range_header[0])
    {
        if (s3->session_token[0])
        {
            snprintf(canonical_headers, sizeof(canonical_headers),
                "host:%s\nrange:%s\nx-amz-content-sha256:%s\n"
                "x-amz-date:%s\nx-amz-security-token:%s\n",
                s3->bucket, range_header, payload_hash,
                amzdate, s3->session_token);
            snprintf(signed_headers, sizeof(signed_headers),
                "host;range;x-amz-content-sha256;x-amz-date;x-amz-security-token");
        }
        else
        {
            snprintf(canonical_headers, sizeof(canonical_headers),
                "host:%s\nrange:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
                s3->bucket, range_header, payload_hash, amzdate);
            snprintf(signed_headers, sizeof(signed_headers),
                "host;range;x-amz-content-sha256;x-amz-date");
        }
    }
    else
    {
        if (s3->session_token[0])
        {
            snprintf(canonical_headers, sizeof(canonical_headers),
                "host:%s\nx-amz-content-sha256:%s\n"
                "x-amz-date:%s\nx-amz-security-token:%s\n",
                s3->bucket, payload_hash, amzdate, s3->session_token);
            snprintf(signed_headers, sizeof(signed_headers),
                "host;x-amz-content-sha256;x-amz-date;x-amz-security-token");
        }
        else
        {
            snprintf(canonical_headers, sizeof(canonical_headers),
                "host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
                s3->bucket, payload_hash, amzdate);
            snprintf(signed_headers, sizeof(signed_headers),
                "host;x-amz-content-sha256;x-amz-date");
        }
    }

    // canonical request
    char canonical_request[4096];
    snprintf(canonical_request, sizeof(canonical_request),
        "%s\n%s\n%s\n%s\n%s\n%s",
        method, uri_path, query_string ? query_string : "",
        canonical_headers, signed_headers, payload_hash);

    // hash of canonical request
    unsigned char cr_hash[32];
    sha256_hash((unsigned char *)canonical_request,
        strlen(canonical_request), cr_hash);
    char cr_hash_hex[65];
    hex_encode(cr_hash, 32, cr_hash_hex);

    // credential scope
    char scope[128];
    snprintf(scope, sizeof(scope), "%s/%s/s3/aws4_request",
        datestamp, s3->region);

    // string to sign
    char string_to_sign[4096];
    snprintf(string_to_sign, sizeof(string_to_sign),
        "AWS4-HMAC-SHA256\n%s\n%s\n%s",
        amzdate, scope, cr_hash_hex);

    // signing key
    char key_buf[128];
    snprintf(key_buf, sizeof(key_buf), "AWS4%s", s3->secret_key);
    unsigned char k_date[32], k_region[32], k_service[32], k_signing[32];
    hmac_sha256((unsigned char *)key_buf, strlen(key_buf),
        (unsigned char *)datestamp, strlen(datestamp), k_date);
    hmac_sha256(k_date, 32,
        (unsigned char *)s3->region, strlen(s3->region), k_region);
    hmac_sha256(k_region, 32,
        (unsigned char *)"s3", 2, k_service);
    hmac_sha256(k_service, 32,
        (unsigned char *)"aws4_request", 12, k_signing);

    // signature
    unsigned char sig[32];
    hmac_sha256(k_signing, 32,
        (unsigned char *)string_to_sign, strlen(string_to_sign), sig);
    char sig_hex[65];
    hex_encode(sig, 32, sig_hex);

    // authorization header
    snprintf(auth_header, auth_size,
        "Authorization: AWS4-HMAC-SHA256 Credential=%s/%s, "
        "SignedHeaders=%s, Signature=%s",
        s3->access_key, scope, signed_headers, sig_hex);
}


// =====================================================================
// S3 backend: read_range
// =====================================================================

static long long s3_read_range(void *backend_data, const char *url,
    long long offset, long long length, unsigned char *buffer)
{
    S3BackendData *s3 = (S3BackendData *)backend_data;
    if (!s3->curl) return -1;

    char range_str[128];
    snprintf(range_str, sizeof(range_str), "bytes=%lld-%lld",
        offset, offset + length - 1);

    // empty payload hash
    static const char *empty_hash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    char auth_hdr[2048], date_hdr[128], token_hdr[1024];
    char range_canon[128];
    snprintf(range_canon, sizeof(range_canon), "bytes=%lld-%lld",
        offset, offset + length - 1);

    aws_sigv4_sign(s3, "GET", s3->object_key, "",
        empty_hash, range_canon, offset, offset + length - 1,
        auth_hdr, sizeof(auth_hdr),
        date_hdr, sizeof(date_hdr),
        token_hdr, sizeof(token_hdr));

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, auth_hdr);
    headers = curl_slist_append(headers, date_hdr);

    char sha_hdr[256];
    snprintf(sha_hdr, sizeof(sha_hdr), "x-amz-content-sha256: %s", empty_hash);
    headers = curl_slist_append(headers, sha_hdr);

    if (token_hdr[0])
        headers = curl_slist_append(headers, token_hdr);

    char range_hdr[256];
    snprintf(range_hdr, sizeof(range_hdr), "Range: %s", range_str);
    headers = curl_slist_append(headers, range_hdr);

    CurlBuffer cb;
    cb.buf = buffer;
    cb.size = 0;
    cb.capacity = length;

    curl_easy_reset(s3->curl);
    curl_easy_setopt(s3->curl, CURLOPT_URL, s3->endpoint);
    curl_easy_setopt(s3->curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(s3->curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(s3->curl, CURLOPT_WRITEDATA, &cb);
    curl_easy_setopt(s3->curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(s3->curl, CURLOPT_FOLLOWLOCATION, 1L);

    CURLcode res = curl_easy_perform(s3->curl);
    curl_slist_free_all(headers);

    if (res != CURLE_OK) return -1;

    long http_code = 0;
    curl_easy_getinfo(s3->curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (http_code != 200 && http_code != 206) return -1;

    return cb.size;
}


// =====================================================================
// S3 backend: get_size (HEAD request)
// =====================================================================

static size_t curl_header_cb(char *buffer, size_t size, size_t nitems,
    void *userdata)
{
    long long *file_size = (long long *)userdata;
    size_t total = size * nitems;

    // look for Content-Length header
    if (total > 16 && strncasecmp(buffer, "content-length:", 15) == 0)
    {
        *file_size = strtoll(buffer + 15, NULL, 10);
    }
    return total;
}

static long long s3_get_size(void *backend_data, const char *url)
{
    S3BackendData *s3 = (S3BackendData *)backend_data;
    if (!s3->curl) return -1;

    static const char *empty_hash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    char auth_hdr[2048], date_hdr[128], token_hdr[1024];
    aws_sigv4_sign(s3, "HEAD", s3->object_key, "",
        empty_hash, NULL, 0, 0,
        auth_hdr, sizeof(auth_hdr),
        date_hdr, sizeof(date_hdr),
        token_hdr, sizeof(token_hdr));

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, auth_hdr);
    headers = curl_slist_append(headers, date_hdr);

    char sha_hdr[256];
    snprintf(sha_hdr, sizeof(sha_hdr), "x-amz-content-sha256: %s", empty_hash);
    headers = curl_slist_append(headers, sha_hdr);

    if (token_hdr[0])
        headers = curl_slist_append(headers, token_hdr);

    long long file_size = -1;

    curl_easy_reset(s3->curl);
    curl_easy_setopt(s3->curl, CURLOPT_URL, s3->endpoint);
    curl_easy_setopt(s3->curl, CURLOPT_NOBODY, 1L);  // HEAD request
    curl_easy_setopt(s3->curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(s3->curl, CURLOPT_HEADERFUNCTION, curl_header_cb);
    curl_easy_setopt(s3->curl, CURLOPT_HEADERDATA, &file_size);
    curl_easy_setopt(s3->curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(s3->curl, CURLOPT_FOLLOWLOCATION, 1L);

    CURLcode res = curl_easy_perform(s3->curl);
    curl_slist_free_all(headers);

    if (res != CURLE_OK) return -1;

    long http_code = 0;
    curl_easy_getinfo(s3->curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (http_code != 200) return -1;

    return file_size;
}


// =====================================================================
// S3 backend: close
// =====================================================================

static void s3_close(void *backend_data)
{
    S3BackendData *s3 = (S3BackendData *)backend_data;
    if (s3)
    {
        if (s3->curl) curl_easy_cleanup(s3->curl);
        free(s3);
    }
}


// =====================================================================
// S3 backend: create
// =====================================================================

/// Parse s3://bucket/key and resolve endpoint
/// Returns S3BackendData* or NULL on error
S3BackendData *s3_backend_create(const char *s3_url,
    const char *access_key, const char *secret_key,
    const char *region, const char *session_token)
{
    // parse s3://bucket/key
    if (strncmp(s3_url, "s3://", 5) != 0) return NULL;
    const char *rest = s3_url + 5;
    const char *slash = strchr(rest, '/');
    if (!slash) return NULL;

    S3BackendData *s3 = (S3BackendData *)calloc(1, sizeof(S3BackendData));
    if (!s3) return NULL;

    // bucket
    size_t bucket_len = (size_t)(slash - rest);
    if (bucket_len >= sizeof(s3->bucket)) bucket_len = sizeof(s3->bucket) - 1;
    strncpy(s3->bucket, rest, bucket_len);
    // Note: s3->bucket stores the S3 host for canonical headers
    // We need a separate variable for the actual bucket name
    // The host for virtual-hosted style is: bucket.s3.region.amazonaws.com

    // object key (starts with /)
    strncpy(s3->object_key, slash, sizeof(s3->object_key) - 1);

    // credentials
    if (access_key && access_key[0])
        strncpy(s3->access_key, access_key, sizeof(s3->access_key) - 1);
    if (secret_key && secret_key[0])
        strncpy(s3->secret_key, secret_key, sizeof(s3->secret_key) - 1);
    if (session_token && session_token[0])
        strncpy(s3->session_token, session_token, sizeof(s3->session_token) - 1);
    if (region && region[0])
        strncpy(s3->region, region, sizeof(s3->region) - 1);
    else
        strncpy(s3->region, "us-east-1", sizeof(s3->region) - 1);

    // build virtual-hosted style endpoint
    char bucket_name[512];
    strncpy(bucket_name, rest, bucket_len);
    bucket_name[bucket_len] = '\0';

    // host for canonical headers
    snprintf(s3->bucket, sizeof(s3->bucket),
        "%s.s3.%s.amazonaws.com", bucket_name, s3->region);

    // full endpoint URL
    snprintf(s3->endpoint, sizeof(s3->endpoint),
        "https://%s%s", s3->bucket, s3->object_key);

    // curl handle
    s3->curl = curl_easy_init();
    if (!s3->curl)
    {
        free(s3);
        return NULL;
    }

    return s3;
}


// =====================================================================
// S3 backend vtable
// =====================================================================

CloudBackend s3_backend_vtable = {
    .read_range = s3_read_range,
    .get_size   = s3_get_size,
    .close      = s3_close
};
