// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// azure_backend.c: Azure Blob Storage backend using libcurl
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
#include <curl/curl.h>

#include <R.h>
#include <Rinternals.h>

// Portable OpenSSL helpers (HMAC-SHA256)
#include "openssl_compat.h"
#include <openssl/bio.h>
#include <openssl/buffer.h>


// =====================================================================
// Azure backend state
// =====================================================================

typedef struct AzureBackendData {
	char account_name[256];
	char account_key[CLOUD_MAX_CRED_LEN];  // base64-encoded
	char sas_token[CLOUD_MAX_CRED_LEN];
	char container[512];
	char blob_name[CLOUD_MAX_URL_LEN];
	char endpoint[CLOUD_MAX_URL_LEN];
	CURL *curl;
} AzureBackendData;


// =====================================================================
// Helper: curl write callback
// =====================================================================

typedef struct {
	unsigned char *buf;
	long long size;
	long long capacity;
} AzureCurlBuffer;

static size_t azure_curl_write_cb(void *data, size_t size, size_t nmemb,
	void *userp)
{
	AzureCurlBuffer *cb = (AzureCurlBuffer *)userp;
	size_t realsize = size * nmemb;
	if (cb->size + (long long)realsize > cb->capacity)
		realsize = (size_t)(cb->capacity - cb->size);
	if (realsize > 0)
	{
		memcpy(cb->buf + cb->size, data, realsize);
		cb->size += realsize;
	}
	return size * nmemb;
}


// =====================================================================
// Helper: header callback
// =====================================================================

static size_t azure_header_cb(char *buffer, size_t size, size_t nitems,
	void *userdata)
{
	long long *file_size = (long long *)userdata;
	size_t total = size * nitems;
	if (total > 16 && strncasecmp(buffer, "content-length:", 15) == 0)
		*file_size = strtoll(buffer + 15, NULL, 10);
	return total;
}


// =====================================================================
// Helper: Base64 decode/encode (for Azure Shared Key)
// =====================================================================

static int base64_decode(const char *input, unsigned char *output,
	size_t *out_len)
{
	BIO *bio, *b64;
	size_t input_len = strlen(input);
	b64 = BIO_new(BIO_f_base64());
	bio = BIO_new_mem_buf(input, (int)input_len);
	bio = BIO_push(b64, bio);
	BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
	*out_len = BIO_read(bio, output, (int)input_len);
	BIO_free_all(bio);
	return (*out_len > 0) ? 0 : -1;
}

static int base64_encode(const unsigned char *input, size_t input_len,
	char *output, size_t out_size)
{
	BIO *bio, *b64;
	BUF_MEM *bptr;
	b64 = BIO_new(BIO_f_base64());
	bio = BIO_new(BIO_s_mem());
	bio = BIO_push(b64, bio);
	BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
	BIO_write(bio, input, (int)input_len);
	BIO_flush(bio);
	BIO_get_mem_ptr(bio, &bptr);
	if (bptr->length < out_size)
	{
		memcpy(output, bptr->data, bptr->length);
		output[bptr->length] = '\0';
	} else {
		BIO_free_all(bio);
		return -1;
	}
	BIO_free_all(bio);
	return 0;
}


// =====================================================================
// Azure Shared Key signature
// =====================================================================

static void azure_sign_request(AzureBackendData *az, const char *method,
	const char *resource_path, const char *range_header,
	char *auth_header, size_t auth_size,
	char *date_header, size_t date_size)
{
	time_t now = time(NULL);
	struct tm utc;
#ifdef _WIN32
	gmtime_s(&utc, &now);
#else
	gmtime_r(&now, &utc);
#endif

	char date_str[64];
	strftime(date_str, sizeof(date_str), "%a, %d %b %Y %H:%M:%S GMT", &utc);
	snprintf(date_header, date_size, "x-ms-date: %s", date_str);

	// string to sign for Blob service
	char string_to_sign[4096];
	snprintf(string_to_sign, sizeof(string_to_sign),
		"%s\n"    // method
		"\n"      // content-encoding
		"\n"      // content-language
		"\n"      // content-length
		"\n"      // content-md5
		"\n"      // content-type
		"\n"      // date
		"\n"      // if-modified-since
		"\n"      // if-match
		"\n"      // if-none-match
		"\n"      // if-unmodified-since
		"%s\n"    // range
		"x-ms-date:%s\n"
		"x-ms-version:2020-10-02\n"
		"/%s%s",
		method,
		range_header ? range_header : "",
		date_str,
		az->account_name, resource_path);

	// decode account key
	unsigned char key_bytes[256];
	size_t key_len = 0;
	base64_decode(az->account_key, key_bytes, &key_len);

	// HMAC-SHA256
	unsigned char sig[32];
	hmac_sha256(key_bytes, key_len,
		(unsigned char *)string_to_sign, strlen(string_to_sign), sig);
	unsigned int sig_len = 32;

	// base64 encode signature
	char sig_b64[128];
	base64_encode(sig, sig_len, sig_b64, sizeof(sig_b64));

	snprintf(auth_header, auth_size,
		"Authorization: SharedKey %s:%s", az->account_name, sig_b64);
}


// =====================================================================
// Azure backend: read_range
// =====================================================================

static long long azure_read_range(void *backend_data, const char *url,
	long long offset, long long length, unsigned char *buffer)
{
	AzureBackendData *az = (AzureBackendData *)backend_data;
	if (!az->curl) return -1;

	struct curl_slist *headers = NULL;

	char range_str[128];
	snprintf(range_str, sizeof(range_str), "bytes=%lld-%lld",
		offset, offset + length - 1);

	char url_str[CLOUD_MAX_URL_LEN];
	strncpy(url_str, az->endpoint, sizeof(url_str) - 1);

	// SAS token or Shared Key
	if (az->sas_token[0])
	{
		// append SAS token to URL
		char sep = (strchr(az->endpoint, '?') != NULL) ? '&' : '?';
		snprintf(url_str, sizeof(url_str), "%s%c%s",
			az->endpoint, sep, az->sas_token);

		char range_hdr[128];
		snprintf(range_hdr, sizeof(range_hdr), "Range: %s", range_str);
		headers = curl_slist_append(headers, range_hdr);
	}
	else if (az->account_key[0])
	{
		char auth_hdr[2048], date_hdr[128];
		char resource_path[CLOUD_MAX_URL_LEN];
		snprintf(resource_path, sizeof(resource_path),
			"/%s/%s", az->container, az->blob_name);
		azure_sign_request(az, "GET", resource_path, range_str,
			auth_hdr, sizeof(auth_hdr), date_hdr, sizeof(date_hdr));
		headers = curl_slist_append(headers, auth_hdr);
		headers = curl_slist_append(headers, date_hdr);
		headers = curl_slist_append(headers, "x-ms-version: 2020-10-02");

		char range_hdr[128];
		snprintf(range_hdr, sizeof(range_hdr), "Range: %s", range_str);
		headers = curl_slist_append(headers, range_hdr);
	}

	AzureCurlBuffer cb;
	cb.buf = buffer;
	cb.size = 0;
	cb.capacity = length;

	curl_easy_reset(az->curl);
	curl_easy_setopt(az->curl, CURLOPT_URL, url_str);
	curl_easy_setopt(az->curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(az->curl, CURLOPT_WRITEFUNCTION, azure_curl_write_cb);
	curl_easy_setopt(az->curl, CURLOPT_WRITEDATA, &cb);
	curl_easy_setopt(az->curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(az->curl, CURLOPT_FOLLOWLOCATION, 1L);

	CURLcode res = curl_easy_perform(az->curl);
	curl_slist_free_all(headers);

	if (res != CURLE_OK) return -1;

	long http_code = 0;
	curl_easy_getinfo(az->curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code != 200 && http_code != 206) return -1;

	return cb.size;
}


// =====================================================================
// Azure backend: get_size
// =====================================================================

static long long azure_get_size(void *backend_data, const char *url)
{
	AzureBackendData *az = (AzureBackendData *)backend_data;
	if (!az->curl) return -1;

	struct curl_slist *headers = NULL;

	char url_str[CLOUD_MAX_URL_LEN];
	strncpy(url_str, az->endpoint, sizeof(url_str) - 1);

	if (az->sas_token[0])
	{
		char sep = (strchr(az->endpoint, '?') != NULL) ? '&' : '?';
		snprintf(url_str, sizeof(url_str), "%s%c%s",
			az->endpoint, sep, az->sas_token);
	}
	else if (az->account_key[0])
	{
		char auth_hdr[2048], date_hdr[128];
		char resource_path[CLOUD_MAX_URL_LEN];
		snprintf(resource_path, sizeof(resource_path),
			"/%s/%s", az->container, az->blob_name);
		azure_sign_request(az, "HEAD", resource_path, NULL,
			auth_hdr, sizeof(auth_hdr), date_hdr, sizeof(date_hdr));
		headers = curl_slist_append(headers, auth_hdr);
		headers = curl_slist_append(headers, date_hdr);
		headers = curl_slist_append(headers, "x-ms-version: 2020-10-02");
	}

	long long file_size = -1;

	curl_easy_reset(az->curl);
	curl_easy_setopt(az->curl, CURLOPT_URL, url_str);
	curl_easy_setopt(az->curl, CURLOPT_NOBODY, 1L);
	curl_easy_setopt(az->curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(az->curl, CURLOPT_HEADERFUNCTION, azure_header_cb);
	curl_easy_setopt(az->curl, CURLOPT_HEADERDATA, &file_size);
	curl_easy_setopt(az->curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(az->curl, CURLOPT_FOLLOWLOCATION, 1L);

	CURLcode res = curl_easy_perform(az->curl);
	curl_slist_free_all(headers);

	if (res != CURLE_OK) return -1;

	long http_code = 0;
	curl_easy_getinfo(az->curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code != 200) return -1;

	return file_size;
}


// =====================================================================
// Azure backend: close
// =====================================================================

static void azure_close(void *backend_data)
{
	AzureBackendData *az = (AzureBackendData *)backend_data;
	if (az)
	{
		if (az->curl) curl_easy_cleanup(az->curl);
		// zero out sensitive data
		memset(az->account_key, 0, sizeof(az->account_key));
		memset(az->sas_token, 0, sizeof(az->sas_token));
		free(az);
	}
}


// =====================================================================
// Azure backend: create
// =====================================================================

/// Parse az://account/container/blob and resolve endpoint
AzureBackendData *azure_backend_create(const char *az_url,
	const char *account_name, const char *account_key,
	const char *sas_token)
{
	// az://container/blob  (account from param or env)
	if (strncmp(az_url, "az://", 5) != 0) return NULL;
	const char *rest = az_url + 5;
	const char *slash = strchr(rest, '/');
	if (!slash) return NULL;

	AzureBackendData *az = (AzureBackendData *)calloc(1, sizeof(AzureBackendData));
	if (!az) return NULL;

	// container
	size_t container_len = (size_t)(slash - rest);
	if (container_len >= sizeof(az->container))
		container_len = sizeof(az->container) - 1;
	strncpy(az->container, rest, container_len);

	// blob name (after container/)
	strncpy(az->blob_name, slash + 1, sizeof(az->blob_name) - 1);

	// credentials
	if (account_name && account_name[0])
		strncpy(az->account_name, account_name, sizeof(az->account_name) - 1);
	if (account_key && account_key[0])
		strncpy(az->account_key, account_key, sizeof(az->account_key) - 1);
	if (sas_token && sas_token[0])
		strncpy(az->sas_token, sas_token, sizeof(az->sas_token) - 1);

	// endpoint
	snprintf(az->endpoint, sizeof(az->endpoint),
		"https://%s.blob.core.windows.net/%s/%s",
		az->account_name, az->container, az->blob_name);

	az->curl = curl_easy_init();
	if (!az->curl)
	{
		free(az);
		return NULL;
	}

	return az;
}


// =====================================================================
// Azure backend vtable
// =====================================================================

CloudBackend azure_backend_vtable = {
	.read_range = azure_read_range,
	.get_size   = azure_get_size,
	.close      = azure_close
};
