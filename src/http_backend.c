// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// http_backend.c: HTTP/HTTPS backend using libcurl (no signing)
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
// HTTP backend state
// =====================================================================

typedef struct HTTPBackendData {
	char url[CLOUD_MAX_URL_LEN];
	char auth_header[CLOUD_MAX_CRED_LEN];  // e.g. "Bearer <token>"
	CURL *curl;
	char last_error[CLOUD_MAX_ERROR_LEN];
} HTTPBackendData;


// =====================================================================
// Helper: curl write callback
// =====================================================================

typedef struct {
	unsigned char *buf;
	long long size;
	long long capacity;
} HTTPCurlBuffer;

static size_t http_curl_write_cb(void *data, size_t size, size_t nmemb,
	void *userp)
{
	HTTPCurlBuffer *cb = (HTTPCurlBuffer *)userp;
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
// Helper: header callback for Content-Range
// =====================================================================

static long long http_parse_content_range_total(const char *value)
{
	const char *slash = strchr(value, '/');
	if (!slash) return -1;
	slash++;
	while (*slash == ' ') slash++;
	if (*slash == '*') return -1;
	return strtoll(slash, NULL, 10);
}

static size_t http_getsize_header_cb(char *buffer, size_t size, size_t nitems,
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
		long long sz = http_parse_content_range_total(value);
		if (sz >= 0) *file_size = sz;
	}
	else if (*file_size < 0 && total > 16 &&
		strncasecmp(buffer, "content-length:", 15) == 0)
	{
		*file_size = strtoll(buffer + 15, NULL, 10);
	}
	return total;
}


// =====================================================================
// HTTP backend: read_range
// =====================================================================

static long long http_read_range(void *backend_data, const char *url,
	long long offset, long long length, unsigned char *buffer)
{
	HTTPBackendData *http = (HTTPBackendData *)backend_data;
	cloud_check_reinit_curl(&http->curl);
	if (!http->curl) return -1;

	struct curl_slist *headers = NULL;

	// authorization header (optional)
	if (http->auth_header[0])
	{
		char auth_hdr[CLOUD_MAX_CRED_LEN + 32];
		snprintf(auth_hdr, sizeof(auth_hdr),
			"Authorization: %s", http->auth_header);
		headers = curl_slist_append(headers, auth_hdr);
	}

	// range header
	char range_hdr[128];
	snprintf(range_hdr, sizeof(range_hdr), "Range: bytes=%lld-%lld",
		offset, offset + length - 1);
	headers = curl_slist_append(headers, range_hdr);

	HTTPCurlBuffer cb;
	cb.buf = buffer;
	cb.size = 0;
	cb.capacity = length;

	curl_easy_reset(http->curl);
	curl_easy_setopt(http->curl, CURLOPT_URL, http->url);
	curl_easy_setopt(http->curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(http->curl, CURLOPT_WRITEFUNCTION, http_curl_write_cb);
	curl_easy_setopt(http->curl, CURLOPT_WRITEDATA, &cb);
	curl_easy_setopt(http->curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(http->curl, CURLOPT_FOLLOWLOCATION, 1L);

	CURLcode res = curl_easy_perform(http->curl);
	curl_slist_free_all(headers);

	if (res != CURLE_OK)
	{
		cloud_format_error(http->last_error, sizeof(http->last_error),
			"HTTP", http->url, res, 0, NULL, 0);
		return -1;
	}

	long http_code = 0;
	curl_easy_getinfo(http->curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code != 200 && http_code != 206)
	{
		cloud_format_error(http->last_error, sizeof(http->last_error),
			"HTTP", http->url, CURLE_OK, http_code, cb.buf, cb.size);
		return -1;
	}

	return cb.size;
}


// =====================================================================
// HTTP backend: get_size
//
// Uses GET with Range: bytes=0-0. On success the server returns
// Content-Range: bytes 0-0/<TOTAL>, from which the file size is parsed.
// Falls back to Content-Length if Content-Range is absent.
// =====================================================================

static long long http_get_size(void *backend_data, const char *url)
{
	HTTPBackendData *http = (HTTPBackendData *)backend_data;
	cloud_check_reinit_curl(&http->curl);
	if (!http->curl) return -1;

	struct curl_slist *headers = NULL;
	if (http->auth_header[0])
	{
		char auth_hdr[CLOUD_MAX_CRED_LEN + 32];
		snprintf(auth_hdr, sizeof(auth_hdr),
			"Authorization: %s", http->auth_header);
		headers = curl_slist_append(headers, auth_hdr);
	}
	headers = curl_slist_append(headers, "Range: bytes=0-0");

	unsigned char body_buf[4096];
	HTTPCurlBuffer body_cb;
	body_cb.buf = body_buf;
	body_cb.size = 0;
	body_cb.capacity = (long long)sizeof(body_buf);

	long long file_size = -1;

	curl_easy_reset(http->curl);
	curl_easy_setopt(http->curl, CURLOPT_URL, http->url);
	curl_easy_setopt(http->curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(http->curl, CURLOPT_WRITEFUNCTION, http_curl_write_cb);
	curl_easy_setopt(http->curl, CURLOPT_WRITEDATA, &body_cb);
	curl_easy_setopt(http->curl, CURLOPT_HEADERFUNCTION, http_getsize_header_cb);
	curl_easy_setopt(http->curl, CURLOPT_HEADERDATA, &file_size);
	curl_easy_setopt(http->curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(http->curl, CURLOPT_FOLLOWLOCATION, 1L);

	CURLcode res = curl_easy_perform(http->curl);
	curl_slist_free_all(headers);

	if (res != CURLE_OK)
	{
		cloud_format_error(http->last_error, sizeof(http->last_error),
			"HTTP", http->url, res, 0, NULL, 0);
		return -1;
	}

	long http_code = 0;
	curl_easy_getinfo(http->curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code != 200 && http_code != 206)
	{
		cloud_format_error(http->last_error, sizeof(http->last_error),
			"HTTP", http->url, CURLE_OK, http_code,
			body_cb.buf, body_cb.size);
		return -1;
	}

	return file_size;
}


// =====================================================================
// HTTP backend: close
// =====================================================================

static void http_close(void *backend_data)
{
	HTTPBackendData *http = (HTTPBackendData *)backend_data;
	if (http)
	{
		if (http->curl) curl_easy_cleanup(http->curl);
		free(http);
	}
}


// =====================================================================
// HTTP backend: create
// =====================================================================

HTTPBackendData *http_backend_create(const char *url,
	const char *auth_header)
{
	if (!url || !url[0]) return NULL;
	if (strncmp(url, "http://", 7) != 0 && strncmp(url, "https://", 8) != 0)
		return NULL;

	HTTPBackendData *http = (HTTPBackendData *)calloc(1, sizeof(HTTPBackendData));
	if (!http) return NULL;

	strncpy(http->url, url, sizeof(http->url) - 1);

	if (auth_header && auth_header[0])
		strncpy(http->auth_header, auth_header, sizeof(http->auth_header) - 1);

	http->curl = curl_easy_init();
	if (!http->curl)
	{
		free(http);
		return NULL;
	}

	return http;
}


// =====================================================================
// HTTP backend: get_last_error
// =====================================================================

static const char *http_get_last_error(void *backend_data)
{
	HTTPBackendData *http = (HTTPBackendData *)backend_data;
	return http ? http->last_error : "";
}


// =====================================================================
// HTTP backend vtable
// =====================================================================

CloudBackend http_backend_vtable = {
	.read_range     = http_read_range,
	.get_size       = http_get_size,
	.close          = http_close,
	.get_last_error = http_get_last_error
};
