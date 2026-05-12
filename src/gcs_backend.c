// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// gcs_backend.c: Google Cloud Storage backend using libcurl
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
#include <curl/curl.h>

#include <R.h>
#include <Rinternals.h>


// =====================================================================
// GCS backend state
// =====================================================================

typedef struct GCSBackendData {
	char access_token[CLOUD_MAX_CRED_LEN];
	char bucket[512];
	char object_key[CLOUD_MAX_URL_LEN];
	char endpoint[CLOUD_MAX_URL_LEN];
	CURL *curl;
	char last_error[CLOUD_MAX_ERROR_LEN];
} GCSBackendData;


// =====================================================================
// Helper: curl write callback
// =====================================================================

typedef struct {
	unsigned char *buf;
	long long size;
	long long capacity;
} GCSCurlBuffer;

static size_t gcs_curl_write_cb(void *data, size_t size, size_t nmemb,
	void *userp)
{
	GCSCurlBuffer *cb = (GCSCurlBuffer *)userp;
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
// Helper: header callback for Content-Length
// =====================================================================

// Parse total size from "Content-Range: bytes 0-0/<TOTAL>".
// Returns -1 if the header is malformed or the total is unknown ("*").
static long long gcs_parse_content_range_total(const char *value)
{
	const char *slash = strchr(value, '/');
	if (!slash) return -1;
	slash++;
	while (*slash == ' ') slash++;
	if (*slash == '*') return -1;
	return strtoll(slash, NULL, 10);
}

// Header callback used by gcs_get_size (GET+Range:0-0).
// Prefers Content-Range's total over Content-Length.
static size_t gcs_getsize_header_cb(char *buffer, size_t size, size_t nitems,
	void *userdata)
{
	long long *file_size = (long long *)userdata;
	size_t total = size * nitems;

	if (total > 14 && strncasecmp(buffer, "content-range:", 14) == 0)
	{
		const char *p = buffer + 14;
		size_t remaining = total - 14;
		while (remaining > 0 && (*p == ' ' || *p == '\t'))
			{ p++; remaining--; }
		char value[128];
		size_t n = (remaining < sizeof(value) - 1) ? remaining : sizeof(value) - 1;
		memcpy(value, p, n);
		value[n] = '\0';
		long long sz = gcs_parse_content_range_total(value);
		if (sz >= 0) *file_size = sz;
	}
	else if (*file_size < 0 && total > 16 &&
		strncasecmp(buffer, "content-length:", 15) == 0)
	{
		// fallback for servers that ignore Range and return the full body
		*file_size = strtoll(buffer + 15, NULL, 10);
	}
	return total;
}


// =====================================================================
// GCS backend: URL-encode the object key
// =====================================================================

static void url_encode_path(const char *src, char *dst, size_t dst_size)
{
	static const char *hex = "0123456789ABCDEF";
	size_t j = 0;
	for (size_t i = 0; src[i] && j < dst_size - 3; i++)
	{
		unsigned char c = (unsigned char)src[i];
		if (c == '/' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~')
		{
			dst[j++] = c;
		} else {
			dst[j++] = '%';
			dst[j++] = hex[(c >> 4) & 0x0f];
			dst[j++] = hex[c & 0x0f];
		}
	}
	dst[j] = '\0';
}


// =====================================================================
// GCS backend: read_range
// =====================================================================

static long long gcs_read_range(void *backend_data, const char *url,
	long long offset, long long length, unsigned char *buffer)
{
	GCSBackendData *gcs = (GCSBackendData *)backend_data;
	cloud_check_reinit_curl(&gcs->curl);
	if (!gcs->curl) return -1;

	struct curl_slist *headers = NULL;

	// authorization header
	if (gcs->access_token[0])
	{
		char auth_hdr[CLOUD_MAX_CRED_LEN + 32];
		snprintf(auth_hdr, sizeof(auth_hdr),
			"Authorization: Bearer %s", gcs->access_token);
		headers = curl_slist_append(headers, auth_hdr);
	}

	// range header
	char range_hdr[128];
	snprintf(range_hdr, sizeof(range_hdr), "Range: bytes=%lld-%lld",
		offset, offset + length - 1);
	headers = curl_slist_append(headers, range_hdr);

	GCSCurlBuffer cb;
	cb.buf = buffer;
	cb.size = 0;
	cb.capacity = length;

	curl_easy_reset(gcs->curl);
	curl_easy_setopt(gcs->curl, CURLOPT_URL, gcs->endpoint);
	curl_easy_setopt(gcs->curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(gcs->curl, CURLOPT_WRITEFUNCTION, gcs_curl_write_cb);
	curl_easy_setopt(gcs->curl, CURLOPT_WRITEDATA, &cb);
	curl_easy_setopt(gcs->curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(gcs->curl, CURLOPT_FOLLOWLOCATION, 1L);

	CURLcode res = curl_easy_perform(gcs->curl);
	curl_slist_free_all(headers);

	if (res != CURLE_OK)
	{
		cloud_format_error(gcs->last_error, sizeof(gcs->last_error),
			"GCS", gcs->endpoint, res, 0, NULL, 0);
		return -1;
	}

	long http_code = 0;
	curl_easy_getinfo(gcs->curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code != 200 && http_code != 206)
	{
		cloud_format_error(gcs->last_error, sizeof(gcs->last_error),
			"GCS", gcs->endpoint, CURLE_OK, http_code, cb.buf, cb.size);
		return -1;
	}

	return cb.size;
}


// =====================================================================
// GCS backend: get_size
//
// Uses GET with Range: bytes=0-0 instead of HEAD so that, on error,
// GCS returns a JSON error body (e.g. {"error":{"code":403,
// "message":"...does not have storage.objects.get access..."}}) that
// can be surfaced to the user. HEAD requests return an empty body on
// error, losing all diagnostic detail. On success, the server returns
// Content-Range: bytes 0-0/<TOTAL>, from which the file size is parsed.
// =====================================================================

static long long gcs_get_size(void *backend_data, const char *url)
{
	GCSBackendData *gcs = (GCSBackendData *)backend_data;
	cloud_check_reinit_curl(&gcs->curl);
	if (!gcs->curl) return -1;

	struct curl_slist *headers = NULL;
	if (gcs->access_token[0])
	{
		char auth_hdr[CLOUD_MAX_CRED_LEN + 32];
		snprintf(auth_hdr, sizeof(auth_hdr),
			"Authorization: Bearer %s", gcs->access_token);
		headers = curl_slist_append(headers, auth_hdr);
	}
	headers = curl_slist_append(headers, "Range: bytes=0-0");

	// Collect the response body: 1 byte on success, JSON error on failure.
	unsigned char body_buf[4096];
	GCSCurlBuffer body_cb;
	body_cb.buf = body_buf;
	body_cb.size = 0;
	body_cb.capacity = (long long)sizeof(body_buf);

	long long file_size = -1;

	curl_easy_reset(gcs->curl);
	curl_easy_setopt(gcs->curl, CURLOPT_URL, gcs->endpoint);
	curl_easy_setopt(gcs->curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(gcs->curl, CURLOPT_WRITEFUNCTION, gcs_curl_write_cb);
	curl_easy_setopt(gcs->curl, CURLOPT_WRITEDATA, &body_cb);
	curl_easy_setopt(gcs->curl, CURLOPT_HEADERFUNCTION, gcs_getsize_header_cb);
	curl_easy_setopt(gcs->curl, CURLOPT_HEADERDATA, &file_size);
	curl_easy_setopt(gcs->curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(gcs->curl, CURLOPT_FOLLOWLOCATION, 1L);

	CURLcode res = curl_easy_perform(gcs->curl);
	curl_slist_free_all(headers);

	if (res != CURLE_OK)
	{
		cloud_format_error(gcs->last_error, sizeof(gcs->last_error),
			"GCS", gcs->endpoint, res, 0, NULL, 0);
		return -1;
	}

	long http_code = 0;
	curl_easy_getinfo(gcs->curl, CURLINFO_RESPONSE_CODE, &http_code);
	// 206 Partial Content for Range:0-0 success; 200 if the server ignored
	// the Range header and returned the full body.
	if (http_code != 200 && http_code != 206)
	{
		cloud_format_error(gcs->last_error, sizeof(gcs->last_error),
			"GCS", gcs->endpoint, CURLE_OK, http_code,
			body_cb.buf, body_cb.size);
		return -1;
	}

	return file_size;
}


// =====================================================================
// GCS backend: close
// =====================================================================

static void gcs_close(void *backend_data)
{
	GCSBackendData *gcs = (GCSBackendData *)backend_data;
	if (gcs)
	{
		if (gcs->curl) curl_easy_cleanup(gcs->curl);
		free(gcs);
	}
}


// =====================================================================
// GCS backend: create
// =====================================================================

/// Parse gs://bucket/key and resolve endpoint
GCSBackendData *gcs_backend_create(const char *gs_url,
	const char *access_token)
{
	if (strncmp(gs_url, "gs://", 5) != 0) return NULL;
	const char *rest = gs_url + 5;
	const char *slash = strchr(rest, '/');
	if (!slash) return NULL;

	GCSBackendData *gcs = (GCSBackendData *)calloc(1, sizeof(GCSBackendData));
	if (!gcs) return NULL;

	// bucket name
	size_t bucket_len = (size_t)(slash - rest);
	if (bucket_len >= sizeof(gcs->bucket)) bucket_len = sizeof(gcs->bucket) - 1;
	strncpy(gcs->bucket, rest, bucket_len);

	// object key (after bucket, without leading /)
	strncpy(gcs->object_key, slash + 1, sizeof(gcs->object_key) - 1);

	// access token
	if (access_token && access_token[0])
		strncpy(gcs->access_token, access_token, sizeof(gcs->access_token) - 1);

	// URL-encode object key for the endpoint
	char encoded_key[CLOUD_MAX_URL_LEN];
	url_encode_path(gcs->object_key, encoded_key, sizeof(encoded_key));

	// GCS JSON API endpoint
	snprintf(gcs->endpoint, sizeof(gcs->endpoint),
		"https://storage.googleapis.com/%s/%s",
		gcs->bucket, encoded_key);

	gcs->curl = curl_easy_init();
	if (!gcs->curl)
	{
		free(gcs);
		return NULL;
	}

	return gcs;
}


// =====================================================================
// GCS backend: get_last_error
// =====================================================================

static const char *gcs_get_last_error(void *backend_data)
{
	GCSBackendData *gcs = (GCSBackendData *)backend_data;
	return gcs ? gcs->last_error : "";
}


// =====================================================================
// GCS backend vtable
// =====================================================================

CloudBackend gcs_backend_vtable = {
	.read_range     = gcs_read_range,
	.get_size       = gcs_get_size,
	.close          = gcs_close,
	.get_last_error = gcs_get_last_error
};
