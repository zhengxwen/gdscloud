// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// gdscloud.c: R .Call entry points for opening cloud GDS files
//
// Copyright (C) 2026    Xiuwen Zheng
//
// This file is part of gdscloud.
// LGPL-3 License
// ===========================================================

#include "cloud_stream.h"
#include <R_GDS.h>
#include <R_GDS_CPP.h>

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

#include <vector>
#include <algorithm>


// Global registry of active CloudStream pointers
static std::vector<CloudStream*> g_open_streams;


// External declarations from C backend files
extern "C" {

typedef struct S3BackendData S3BackendData;
extern S3BackendData *s3_backend_create(const char *s3_url,
	const char *access_key, const char *secret_key,
	const char *region, const char *session_token);
extern CloudBackend s3_backend_vtable;

typedef struct GCSBackendData GCSBackendData;
extern GCSBackendData *gcs_backend_create(const char *gs_url,
	const char *access_token);
extern CloudBackend gcs_backend_vtable;

typedef struct AzureBackendData AzureBackendData;
extern AzureBackendData *azure_backend_create(const char *az_url,
	const char *account_name, const char *account_key,
	const char *sas_token);
extern CloudBackend azure_backend_vtable;

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
// Helper: get a non-NA, non-empty string from SEXP, or return ""
// =====================================================================

static const char *sexp_str(SEXP x)
{
	if (TYPEOF(x) != STRSXP || XLENGTH(x) == 0) return "";
	SEXP s = STRING_ELT(x, 0);
	if (s == NA_STRING) return "";
	return CHAR(s);
}


// =====================================================================
// Helper: build the gds.class R list from a PdGDSFile
// =====================================================================

// =====================================================================
// .Call: Open S3 GDS file
// =====================================================================

extern "C" SEXP gdscloud_open_s3(SEXP url, SEXP access_key, SEXP secret_key,
	SEXP region, SEXP session_token, SEXP cache_size_mb)
{
	const char *c_url = sexp_str(url);
	const char *c_ak  = sexp_str(access_key);
	const char *c_sk  = sexp_str(secret_key);
	const char *c_rgn = sexp_str(region);
	const char *c_tok = sexp_str(session_token);
	double c_cache = Rf_asReal(cache_size_mb);

	COREARRAY_TRY

		// validate URL format
		if (!c_url || !c_url[0])
			throw ErrGDSCloud("S3 URL is empty or missing");
		if (strncmp(c_url, "s3://", 5) != 0)
			throw ErrGDSCloud("Invalid S3 URL '%s': must start with 's3://'", c_url);
		if (!strchr(c_url + 5, '/'))
			throw ErrGDSCloud("Invalid S3 URL '%s': missing object key (expected 's3://bucket/key')", c_url);

		// create S3 backend
		S3BackendData *s3 = s3_backend_create(c_url, c_ak, c_sk, c_rgn, c_tok);
		if (!s3)
			throw ErrGDSCloud("Failed to create S3 backend for '%s'", c_url);

		// create cloud stream with cache
		long long max_cache = (long long)(c_cache * 1024 * 1024);
		CloudStream *cs = cloud_stream_create(c_url,
			&s3_backend_vtable, s3, CLOUD_BLOCK_SIZE, max_cache);
		if (!cs)
		{
			s3_backend_vtable.close(s3);
			throw ErrGDSCloud("Failed to create cloud stream for '%s'", c_url);
		}

		// pre-check file access to get detailed error on failure
		if (cloud_stream_getsize(cs) < 0)
		{
			const char *err = cloud_stream_get_last_error(cs);
			std::string msg = (err && err[0]) ? std::string(err)
				: std::string("Failed to access '") + c_url + "'";
			cloud_stream_close(cs);
			throw ErrGDSCloud(msg);
		}

		// open via gdsfmt callback API
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
				"the file may not exist, access may be denied, or it is not a valid GDS file", c_url);
		}

		g_open_streams.push_back(cs);
		rv_ans = GDS_R_MakeFileObj(file, c_url, TRUE);

	COREARRAY_CATCH
}


// =====================================================================
// .Call: Open GCS GDS file
// =====================================================================

extern "C" SEXP gdscloud_open_gcs(SEXP url, SEXP access_token, SEXP cache_size_mb)
{
	const char *c_url = sexp_str(url);
	const char *c_tok = sexp_str(access_token);
	double c_cache = Rf_asReal(cache_size_mb);

	COREARRAY_TRY

		// validate URL format
		if (!c_url || !c_url[0])
			throw ErrGDSCloud("GCS URL is empty or missing");
		if (strncmp(c_url, "gs://", 5) != 0)
			throw ErrGDSCloud("Invalid GCS URL '%s': must start with 'gs://'", c_url);
		if (!strchr(c_url + 5, '/'))
			throw ErrGDSCloud("Invalid GCS URL '%s': missing object key (expected 'gs://bucket/key')", c_url);

		GCSBackendData *gcs = gcs_backend_create(c_url, c_tok);
		if (!gcs)
			throw ErrGDSCloud("Failed to create GCS backend for '%s'", c_url);

		long long max_cache = (long long)(c_cache * 1024 * 1024);
		CloudStream *cs = cloud_stream_create(c_url,
			&gcs_backend_vtable, gcs, CLOUD_BLOCK_SIZE, max_cache);
		if (!cs)
		{
			gcs_backend_vtable.close(gcs);
			throw ErrGDSCloud("Failed to create cloud stream for '%s'", c_url);
		}

		// pre-check file access to get detailed error on failure
		if (cloud_stream_getsize(cs) < 0)
		{
			const char *err = cloud_stream_get_last_error(cs);
			std::string msg = (err && err[0]) ? std::string(err)
				: std::string("Failed to access '") + c_url + "'";
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
				"the file may not exist, access may be denied, or it is not a valid GDS file", c_url);
		}

		g_open_streams.push_back(cs);
		rv_ans = GDS_R_MakeFileObj(file, c_url, TRUE);

	COREARRAY_CATCH
}


// =====================================================================
// .Call: Open Azure GDS file
// =====================================================================

extern "C" SEXP gdscloud_open_azure(SEXP url, SEXP account_name, SEXP account_key,
	SEXP sas_token, SEXP cache_size_mb)
{
	const char *c_url = sexp_str(url);
	const char *c_acc = sexp_str(account_name);
	const char *c_key = sexp_str(account_key);
	const char *c_sas = sexp_str(sas_token);
	double c_cache = Rf_asReal(cache_size_mb);

	COREARRAY_TRY

		// validate URL format
		if (!c_url || !c_url[0])
			throw ErrGDSCloud("Azure URL is empty or missing");
		if (strncmp(c_url, "az://", 5) != 0)
			throw ErrGDSCloud("Invalid Azure URL '%s': must start with 'az://'", c_url);
		if (!strchr(c_url + 5, '/'))
			throw ErrGDSCloud("Invalid Azure URL '%s': missing blob name (expected 'az://container/blob')", c_url);
		if (!c_acc || !c_acc[0])
			throw ErrGDSCloud("Azure account name is required for '%s'", c_url);

		AzureBackendData *az = azure_backend_create(c_url, c_acc, c_key, c_sas);
		if (!az)
			throw ErrGDSCloud("Failed to create Azure backend for '%s'", c_url);

		long long max_cache = (long long)(c_cache * 1024 * 1024);
		CloudStream *cs = cloud_stream_create(c_url,
			&azure_backend_vtable, az, CLOUD_BLOCK_SIZE, max_cache);
		if (!cs)
		{
			azure_backend_vtable.close(az);
			throw ErrGDSCloud("Failed to create cloud stream for '%s'", c_url);
		}

		// pre-check file access to get detailed error on failure
		if (cloud_stream_getsize(cs) < 0)
		{
			const char *err = cloud_stream_get_last_error(cs);
			std::string msg = (err && err[0]) ? std::string(err)
				: std::string("Failed to access '") + c_url + "'";
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
				"the file may not exist, access may be denied, or it is not a valid GDS file", c_url);
		}

		g_open_streams.push_back(cs);
		rv_ans = GDS_R_MakeFileObj(file, c_url, TRUE);

	COREARRAY_CATCH
}


// =====================================================================
// .Call: Clear cache (placeholder - individual stream caches are
//        freed when streams close; this is a no-op for now)
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

	SEXP ans = PROTECT(Rf_allocVector(VECSXP, 3));
	SEXP names = PROTECT(Rf_allocVector(STRSXP, 3));
	SET_STRING_ELT(names, 0, Rf_mkChar("num_streams"));
	SET_STRING_ELT(names, 1, Rf_mkChar("hits"));
	SET_STRING_ELT(names, 2, Rf_mkChar("misses"));
	Rf_setAttrib(ans, R_NamesSymbol, names);

	SET_VECTOR_ELT(ans, 0, Rf_ScalarInteger((int)g_open_streams.size()));
	SET_VECTOR_ELT(ans, 1, Rf_ScalarReal((double)total_hits));
	SET_VECTOR_ELT(ans, 2, Rf_ScalarReal((double)total_misses));

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
