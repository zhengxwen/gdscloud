// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// gdscloud.cpp: R .Call entry points for opening cloud GDS files
//
// Copyright (C) 2026    Xiuwen Zheng
//
// This file is part of gdscloud.
// LGPL-3 License
// ===========================================================

#include "cloud_stream.h"
#include "curl_backend.h"
#include <R_GDS.h>
#include <R_GDS_CPP.h>

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

#include <vector>
#include <string>
#include <algorithm>


// Global registry of active CloudStream pointers
static std::vector<CloudStream*> g_open_streams;


// Providers implemented in the C files
extern "C" {

extern const CurlProvider http_provider;
extern void *http_provider_create(const char *url, const char *auth,
	char *err, size_t err_size);

extern const CurlProvider s3_provider;
extern void *s3_provider_create(const char *url, const char *access_key,
	const char *secret_key, const char *region, const char *session_token,
	const char *endpoint, int path_style, char *err, size_t err_size);

extern const CurlProvider gcs_provider;
extern void *gcs_provider_create(const char *url, const char *access_token,
	char *err, size_t err_size);

extern const CurlProvider azure_provider;
extern void *azure_provider_create(const char *url, const char *account_name,
	const char *account_key, const char *sas_token, const char *access_token,
	const char *endpoint_suffix, const char *endpoint,
	char *err, size_t err_size);

} // extern "C"


// ===========================================================
// Define Exception
// ===========================================================

using namespace CoreArray;

class ErrGDSCloud: public ErrCoreArray
{
public:
	ErrGDSCloud(): ErrCoreArray()
		{ }
	ErrGDSCloud(const char *fmt, ...): ErrCoreArray()
		{ _COREARRAY_ERRMACRO_(fmt); }
	ErrGDSCloud(const std::string &msg): ErrCoreArray()
		{ fMessage = msg; }
};


// =====================================================================
// Callback functions matching gdsfmt's TdCbStream* signatures
// =====================================================================

/// Read callback: user_data is a CloudStream*
static ssize_t gdscloud_cb_read(void *user_data, void *buffer, ssize_t count)
{
	CloudStream *cs = (CloudStream *)user_data;
	long long result = cloud_stream_read(cs, buffer, (long long)count);
	if (result < 0)
	{
		const char *err = cloud_stream_get_last_error(cs);
		if (err && err[0])
			throw ErrGDSCloud("Cloud stream read error: %s", err);
		else
			throw ErrGDSCloud("Cloud stream read error");
	}
	return (ssize_t)result;
}

/// Seek callback: origin matches TdSysSeekOrg (0=begin, 1=current, 2=end)
static long long gdscloud_cb_seek(void *user_data, long long offset, int origin)
{
	CloudStream *cs = (CloudStream *)user_data;
	return cloud_stream_seek(cs, offset, origin);
}

/// GetSize callback
static long long gdscloud_cb_getsize(void *user_data)
{
	CloudStream *cs = (CloudStream *)user_data;
	long long result = cloud_stream_getsize(cs);
	if (result < 0)
	{
		const char *err = cloud_stream_get_last_error(cs);
		if (err && err[0])
			throw ErrGDSCloud("Cloud stream get size error: %s", err);
		else
			throw ErrGDSCloud("Cloud stream get size error");
	}
	return result;
}

/// Close callback
static void gdscloud_cb_close(void *user_data)
{
	CloudStream *cs = (CloudStream *)user_data;
	g_open_streams.erase(
		std::remove(g_open_streams.begin(), g_open_streams.end(), cs),
		g_open_streams.end());
	cloud_stream_close(cs);
}


// =====================================================================
// Helpers
// =====================================================================

/// a non-NA, non-empty string from a character SEXP, or ""
static const char *sexp_str(SEXP x)
{
	if (TYPEOF(x) != STRSXP || XLENGTH(x) == 0) return "";
	SEXP s = STRING_ELT(x, 0);
	if (s == NA_STRING) return "";
	return CHAR(s);
}

/// a three-state flag from a character SEXP: "TRUE" -> 1, "FALSE" -> 0,
/// anything else (including "" and NA) -> -1 (automatic)
static int sexp_flag(SEXP x)
{
	const char *s = sexp_str(x);
	if (strcmp(s, "TRUE") == 0) return 1;
	if (strcmp(s, "FALSE") == 0) return 0;
	return -1;
}

/// set attr(file_obj$filename, "pkgname") <- "gdscloud"
static void set_pkgname_attr(SEXP file_obj)
{
	SEXP names = Rf_getAttrib(file_obj, R_NamesSymbol);
	if (TYPEOF(names) == STRSXP)
	{
		for (R_xlen_t i = 0; i < Rf_xlength(file_obj); i++)
		{
			if (strcmp(CHAR(STRING_ELT(names, i)), "filename") == 0)
			{
				SEXP val = PROTECT(Rf_mkString("gdscloud"));
				Rf_setAttrib(VECTOR_ELT(file_obj, i),
					Rf_install("pkgname"), val);
				UNPROTECT(1);
				break;
			}
		}
	}
}

/// The common part of every open: wrap the provider in the generic
/// backend, create the cached stream, pre-check access (for a detailed
/// error message) and open through gdsfmt's callback-stream API.
/// Takes ownership of `provider_data`.
static SEXP open_cloud_gds(const char *url, const CurlProvider *provider,
	void *provider_data, double cache_mb)
{
	CurlBackendData *bd = curl_backend_create(provider, provider_data);
	if (!bd)
		throw ErrGDSCloud("Failed to initialize libcurl for '%s'", url);

	long long max_cache = (long long)(cache_mb * 1024 * 1024);
	CloudStream *cs = cloud_stream_create(url, &curl_backend_vtable, bd,
		CLOUD_BLOCK_SIZE, max_cache);
	if (!cs)
	{
		curl_backend_vtable.close(bd);
		throw ErrGDSCloud("Failed to create cloud stream for '%s'", url);
	}

	if (cloud_stream_getsize(cs) < 0)
	{
		const char *err = cloud_stream_get_last_error(cs);
		std::string msg = (err && err[0]) ? std::string(err)
			: std::string("Failed to access '") + url + "'";
		cloud_stream_close(cs);
		throw ErrGDSCloud(msg);
	}

	PdGDSFile file = GDS_File_Open_Callback(
		cs,
		(TdCbStreamRead)gdscloud_cb_read,
		(TdCbStreamWrite)NULL,
		(TdCbStreamSeek)gdscloud_cb_seek,
		(TdCbStreamGetSize)gdscloud_cb_getsize,
		(TdCbStreamSetSize)NULL,
		(TdCbStreamClose)gdscloud_cb_close,
		TRUE, FALSE);
	if (!file)
	{
		cloud_stream_close(cs);
		throw ErrGDSCloud("Failed to open GDS file from '%s': "
			"the file may not exist, access may be denied, or it is not a "
			"valid GDS file", url);
	}

	g_open_streams.push_back(cs);
	SEXP ans = PROTECT(GDS_R_MakeFileObj(file, url, TRUE));
	set_pkgname_attr(ans);
	UNPROTECT(1);
	return ans;
}


// =====================================================================
// .Call: Open HTTP/HTTPS GDS file
// =====================================================================

extern "C" SEXP gdscloud_open_http(SEXP url, SEXP auth_header,
	SEXP cache_size_mb)
{
	const char *c_url = sexp_str(url);
	COREARRAY_TRY
		if (!c_url[0])
			throw ErrGDSCloud("HTTP URL is empty or missing");
		char err[256];
		void *pd = http_provider_create(c_url, sexp_str(auth_header),
			err, sizeof(err));
		if (!pd)
			throw ErrGDSCloud("Invalid HTTP URL '%s': %s", c_url, err);
		rv_ans = open_cloud_gds(c_url, &http_provider, pd,
			Rf_asReal(cache_size_mb));
	COREARRAY_CATCH
}


// =====================================================================
// .Call: Open S3 GDS file
// =====================================================================

extern "C" SEXP gdscloud_open_s3(SEXP url, SEXP access_key, SEXP secret_key,
	SEXP region, SEXP session_token, SEXP endpoint, SEXP path_style,
	SEXP cache_size_mb)
{
	const char *c_url = sexp_str(url);
	COREARRAY_TRY
		if (!c_url[0])
			throw ErrGDSCloud("S3 URL is empty or missing");
		char err[512];
		void *pd = s3_provider_create(c_url, sexp_str(access_key),
			sexp_str(secret_key), sexp_str(region), sexp_str(session_token),
			sexp_str(endpoint), sexp_flag(path_style), err, sizeof(err));
		if (!pd)
			throw ErrGDSCloud("Invalid S3 URL '%s': %s", c_url, err);
		rv_ans = open_cloud_gds(c_url, &s3_provider, pd,
			Rf_asReal(cache_size_mb));
	COREARRAY_CATCH
}


// =====================================================================
// .Call: Open GCS GDS file
// =====================================================================

extern "C" SEXP gdscloud_open_gcs(SEXP url, SEXP access_token, SEXP cache_size_mb)
{
	const char *c_url = sexp_str(url);
	COREARRAY_TRY
		if (!c_url[0])
			throw ErrGDSCloud("GCS URL is empty or missing");
		char err[256];
		void *pd = gcs_provider_create(c_url, sexp_str(access_token),
			err, sizeof(err));
		if (!pd)
			throw ErrGDSCloud("Invalid GCS URL '%s': %s", c_url, err);
		rv_ans = open_cloud_gds(c_url, &gcs_provider, pd,
			Rf_asReal(cache_size_mb));
	COREARRAY_CATCH
}


// =====================================================================
// .Call: Open Azure GDS file
// =====================================================================

extern "C" SEXP gdscloud_open_azure(SEXP url, SEXP account_name, SEXP account_key,
	SEXP sas_token, SEXP access_token, SEXP endpoint_suffix, SEXP endpoint,
	SEXP cache_size_mb)
{
	const char *c_url = sexp_str(url);
	COREARRAY_TRY
		if (!c_url[0])
			throw ErrGDSCloud("Azure URL is empty or missing");
		char err[512];
		void *pd = azure_provider_create(c_url, sexp_str(account_name),
			sexp_str(account_key), sexp_str(sas_token), sexp_str(access_token),
			sexp_str(endpoint_suffix), sexp_str(endpoint), err, sizeof(err));
		if (!pd)
			throw ErrGDSCloud("Cannot open '%s': %s", c_url, err);
		rv_ans = open_cloud_gds(c_url, &azure_provider, pd,
			Rf_asReal(cache_size_mb));
	COREARRAY_CATCH
}


// =====================================================================
// .Call: Build the request a provider would send, without sending it
//   gdscloud_prepare_request(url, params, range, time)
// `params` is a named character list of provider credentials; used by
// the unit tests to check signatures against published test vectors.
// Returns list(url=, headers=).
// =====================================================================

static const char *param(SEXP lst, const char *name)
{
	if (TYPEOF(lst) != VECSXP) return "";
	SEXP names = Rf_getAttrib(lst, R_NamesSymbol);
	if (TYPEOF(names) != STRSXP) return "";
	for (R_xlen_t i = 0; i < Rf_xlength(lst); i++)
	{
		if (strcmp(CHAR(STRING_ELT(names, i)), name) == 0)
			return sexp_str(VECTOR_ELT(lst, i));
	}
	return "";
}

extern "C" SEXP gdscloud_prepare_request(SEXP url, SEXP params, SEXP range,
	SEXP time_utc)
{
	const char *c_url = sexp_str(url);
	COREARRAY_TRY
		const CurlProvider *provider = NULL;
		void *pd = NULL;
		char err[512];
		if (strncmp(c_url, "http://", 7) == 0 || strncmp(c_url, "https://", 8) == 0)
		{
			provider = &http_provider;
			pd = http_provider_create(c_url, param(params, "auth"), err, sizeof(err));
		}
		else if (strncmp(c_url, "s3://", 5) == 0)
		{
			provider = &s3_provider;
			int ps = -1;
			if (strcmp(param(params, "path_style"), "TRUE") == 0) ps = 1;
			else if (strcmp(param(params, "path_style"), "FALSE") == 0) ps = 0;
			pd = s3_provider_create(c_url, param(params, "access_key"),
				param(params, "secret_key"), param(params, "region"),
				param(params, "session_token"), param(params, "endpoint"),
				ps, err, sizeof(err));
		}
		else if (strncmp(c_url, "gs://", 5) == 0)
		{
			provider = &gcs_provider;
			pd = gcs_provider_create(c_url, param(params, "access_token"),
				err, sizeof(err));
		}
		else if (strncmp(c_url, "az://", 5) == 0)
		{
			provider = &azure_provider;
			pd = azure_provider_create(c_url, param(params, "account_name"),
				param(params, "account_key"), param(params, "sas_token"),
				param(params, "access_token"), param(params, "endpoint_suffix"),
				param(params, "endpoint"), err, sizeof(err));
		}
		else
			throw ErrGDSCloud("Unsupported URL '%s'", c_url);
		if (!pd)
			throw ErrGDSCloud("Invalid URL '%s': %s", c_url, err);

		CurlRequest req;
		req.url[0] = '\0';
		req.headers = NULL;
		err[0] = '\0';
		int rc = provider->prepare(pd, sexp_str(range),
			(time_t)Rf_asReal(time_utc), &req, err, sizeof(err));
		std::vector<std::string> hdrs;
		for (struct curl_slist *p = req.headers; p; p = p->next)
			hdrs.push_back(p->data);
		std::string req_url(req.url);
		if (req.headers) curl_slist_free_all(req.headers);
		provider->free_data(pd);
		if (rc != 0)
			throw ErrGDSCloud("%s: %s", provider->name, err);

		rv_ans = PROTECT(Rf_allocVector(VECSXP, 2));
		SEXP names = PROTECT(Rf_allocVector(STRSXP, 2));
		SET_STRING_ELT(names, 0, Rf_mkChar("url"));
		SET_STRING_ELT(names, 1, Rf_mkChar("headers"));
		Rf_setAttrib(rv_ans, R_NamesSymbol, names);
		SET_VECTOR_ELT(rv_ans, 0, Rf_mkString(req_url.c_str()));
		SEXP hv = PROTECT(Rf_allocVector(STRSXP, hdrs.size()));
		for (size_t i = 0; i < hdrs.size(); i++)
			SET_STRING_ELT(hv, i, Rf_mkChar(hdrs[i].c_str()));
		SET_VECTOR_ELT(rv_ans, 1, hv);
		UNPROTECT(3);
	COREARRAY_CATCH
}


// =====================================================================
// .Call: Set the transfer options (timeouts in seconds, max retries)
// =====================================================================

extern "C" SEXP gdscloud_set_options(SEXP connect_timeout, SEXP timeout,
	SEXP max_retries)
{
	double ct = Rf_asReal(connect_timeout), lt = Rf_asReal(timeout);
	double mr = Rf_asReal(max_retries);
	cloud_set_timeouts(
		(R_FINITE(ct) && ct > 0) ? (long)ct : 0,
		(R_FINITE(lt) && lt > 0) ? (long)lt : 0);
	cloud_set_max_retries((R_FINITE(mr) && mr >= 0) ? (int)mr : -1);
	return R_NilValue;
}


// =====================================================================
// .Call: Clear the block caches of all open cloud streams
// =====================================================================

extern "C" SEXP gdscloud_cache_clear(void)
{
	for (size_t i = 0; i < g_open_streams.size(); i++)
		cache_clear_all(&g_open_streams[i]->cache);
	return R_NilValue;
}


// =====================================================================
// .Call: Cache info
// =====================================================================

extern "C" SEXP gdscloud_cache_info(void)
{
	long long total_hits = 0, total_misses = 0;
	for (size_t i = 0; i < g_open_streams.size(); i++)
	{
		total_hits   += g_open_streams[i]->cache.total_hits;
		total_misses += g_open_streams[i]->cache.total_misses;
	}

	SEXP ans = PROTECT(Rf_allocVector(VECSXP, 4));
	SEXP names = PROTECT(Rf_allocVector(STRSXP, 4));
	SET_STRING_ELT(names, 0, Rf_mkChar("num_streams"));
	SET_STRING_ELT(names, 1, Rf_mkChar("hits"));
	SET_STRING_ELT(names, 2, Rf_mkChar("misses"));
	SET_STRING_ELT(names, 3, Rf_mkChar("retries"));
	Rf_setAttrib(ans, R_NamesSymbol, names);

	SET_VECTOR_ELT(ans, 0, Rf_ScalarInteger((int)g_open_streams.size()));
	SET_VECTOR_ELT(ans, 1, Rf_ScalarReal((double)total_hits));
	SET_VECTOR_ELT(ans, 2, Rf_ScalarReal((double)total_misses));
	SET_VECTOR_ELT(ans, 3, Rf_ScalarReal((double)cloud_total_retries()));

	UNPROTECT(2);
	return ans;
}


// =====================================================================
// .Call: List open cloud streams with per-stream details
// =====================================================================

extern "C" SEXP gdscloud_list_streams(void)
{
	int n = (int)g_open_streams.size();

	// create a data.frame-like list with 5 columns
	SEXP ans = PROTECT(Rf_allocVector(VECSXP, 5));
	SEXP col_url      = PROTECT(Rf_allocVector(STRSXP, n));
	SEXP col_filesize  = PROTECT(Rf_allocVector(REALSXP, n));
	SEXP col_cacheblk  = PROTECT(Rf_allocVector(INTSXP, n));
	SEXP col_hits      = PROTECT(Rf_allocVector(REALSXP, n));
	SEXP col_misses    = PROTECT(Rf_allocVector(REALSXP, n));

	for (int i = 0; i < n; i++)
	{
		CloudStream *cs = g_open_streams[i];
		SET_STRING_ELT(col_url, i, Rf_mkChar(cs->url));
		REAL(col_filesize)[i] = (double)cs->file_size;
		INTEGER(col_cacheblk)[i] = cs->cache.num_blocks;
		REAL(col_hits)[i] = (double)cs->cache.total_hits;
		REAL(col_misses)[i] = (double)cs->cache.total_misses;
	}

	SET_VECTOR_ELT(ans, 0, col_url);
	SET_VECTOR_ELT(ans, 1, col_filesize);
	SET_VECTOR_ELT(ans, 2, col_cacheblk);
	SET_VECTOR_ELT(ans, 3, col_hits);
	SET_VECTOR_ELT(ans, 4, col_misses);

	// set column names
	SEXP names = PROTECT(Rf_allocVector(STRSXP, 5));
	SET_STRING_ELT(names, 0, Rf_mkChar("url"));
	SET_STRING_ELT(names, 1, Rf_mkChar("file_size"));
	SET_STRING_ELT(names, 2, Rf_mkChar("cache_blocks"));
	SET_STRING_ELT(names, 3, Rf_mkChar("cache_hits"));
	SET_STRING_ELT(names, 4, Rf_mkChar("cache_misses"));
	Rf_setAttrib(ans, R_NamesSymbol, names);

	// set class to data.frame
	SEXP cls = PROTECT(Rf_mkString("data.frame"));
	Rf_setAttrib(ans, R_ClassSymbol, cls);

	// set row.names (1:n)
	SEXP rownames = PROTECT(Rf_allocVector(INTSXP, 2));
	INTEGER(rownames)[0] = NA_INTEGER;
	INTEGER(rownames)[1] = -n;
	Rf_setAttrib(ans, R_RowNamesSymbol, rownames);

	UNPROTECT(9);
	return ans;
}
