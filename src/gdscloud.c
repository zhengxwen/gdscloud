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

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>


// External declarations from backend files
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

// External declarations from gds_callbacks.c
extern ssize_t gdscloud_cb_read(void *buffer, ssize_t count, void *user_data);
extern long long gdscloud_cb_seek(long long offset, int origin, void *user_data);
extern long long gdscloud_cb_getsize(void *user_data);
extern void gdscloud_cb_close(void *user_data);


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

static SEXP make_gds_class(PdGDSFile file, const char *url)
{
    // mimics the return structure of gdsOpenGDS
    SEXP ans = PROTECT(Rf_allocVector(VECSXP, 5));

    // filename (the cloud URL)
    SET_VECTOR_ELT(ans, 0, Rf_mkString(url));

    // id (file index, query from gdsfmt internals is tricky;
    // we use -1 as placeholder since the file is managed by gdsfmt)
    // Actually GDS_File_Open_Callback stores in PKG_GDS_Files,
    // but we don't have direct access to the index from here.
    // The gds.class uses the ptr for most operations.
    SET_VECTOR_ELT(ans, 1, Rf_ScalarInteger(-1));

    // ptr (external pointer to the GDS file)
    SEXP ptr = PROTECT(R_MakeExternalPtr(file, R_NilValue, R_NilValue));
    SET_VECTOR_ELT(ans, 2, ptr);

    // root (external pointer to root folder)
    PdGDSFolder root = GDS_File_Root(file);
    SET_VECTOR_ELT(ans, 3, GDS_R_Obj2SEXP((PdGDSObj)root));

    // readonly
    SET_VECTOR_ELT(ans, 4, Rf_ScalarLogical(TRUE));

    UNPROTECT(2);
    return ans;
}


// =====================================================================
// .Call: Open S3 GDS file
// =====================================================================

SEXP gdscloud_open_s3(SEXP url, SEXP access_key, SEXP secret_key,
    SEXP region, SEXP session_token, SEXP cache_size_mb)
{
    const char *c_url = sexp_str(url);
    const char *c_ak  = sexp_str(access_key);
    const char *c_sk  = sexp_str(secret_key);
    const char *c_rgn = sexp_str(region);
    const char *c_tok = sexp_str(session_token);
    double c_cache = Rf_asReal(cache_size_mb);

    // create S3 backend
    S3BackendData *s3 = s3_backend_create(c_url, c_ak, c_sk, c_rgn, c_tok);
    if (!s3)
        Rf_error("Failed to create S3 backend for '%s'", c_url);

    // create cloud stream with cache
    long long max_cache = (long long)(c_cache * 1024 * 1024);
    CloudStream *cs = cloud_stream_create(c_url,
        &s3_backend_vtable, s3, CLOUD_BLOCK_SIZE, max_cache);
    if (!cs)
    {
        s3_backend_vtable.close(s3);
        Rf_error("Failed to create cloud stream for '%s'", c_url);
    }

    // open via gdsfmt callback API
    PdGDSFile file = GDS_File_Open_Callback(
        (TdCbStreamRead)gdscloud_cb_read,
        (TdCbStreamWrite)NULL,
        (TdCbStreamSeek)gdscloud_cb_seek,
        (TdCbStreamGetSize)gdscloud_cb_getsize,
        (TdCbStreamSetSize)NULL,
        (TdCbStreamClose)gdscloud_cb_close,
        cs, TRUE, FALSE);

    if (!file)
    {
        cloud_stream_close(cs);
        Rf_error("Failed to open GDS file from '%s'", c_url);
    }

    return make_gds_class(file, c_url);
}


// =====================================================================
// .Call: Open GCS GDS file
// =====================================================================

SEXP gdscloud_open_gcs(SEXP url, SEXP access_token, SEXP cache_size_mb)
{
    const char *c_url = sexp_str(url);
    const char *c_tok = sexp_str(access_token);
    double c_cache = Rf_asReal(cache_size_mb);

    GCSBackendData *gcs = gcs_backend_create(c_url, c_tok);
    if (!gcs)
        Rf_error("Failed to create GCS backend for '%s'", c_url);

    long long max_cache = (long long)(c_cache * 1024 * 1024);
    CloudStream *cs = cloud_stream_create(c_url,
        &gcs_backend_vtable, gcs, CLOUD_BLOCK_SIZE, max_cache);
    if (!cs)
    {
        gcs_backend_vtable.close(gcs);
        Rf_error("Failed to create cloud stream for '%s'", c_url);
    }

    PdGDSFile file = GDS_File_Open_Callback(
        (TdCbStreamRead)gdscloud_cb_read,
        (TdCbStreamWrite)NULL,
        (TdCbStreamSeek)gdscloud_cb_seek,
        (TdCbStreamGetSize)gdscloud_cb_getsize,
        (TdCbStreamSetSize)NULL,
        (TdCbStreamClose)gdscloud_cb_close,
        cs, TRUE, FALSE);

    if (!file)
    {
        cloud_stream_close(cs);
        Rf_error("Failed to open GDS file from '%s'", c_url);
    }

    return make_gds_class(file, c_url);
}


// =====================================================================
// .Call: Open Azure GDS file
// =====================================================================

SEXP gdscloud_open_azure(SEXP url, SEXP account_name, SEXP account_key,
    SEXP sas_token, SEXP cache_size_mb)
{
    const char *c_url = sexp_str(url);
    const char *c_acc = sexp_str(account_name);
    const char *c_key = sexp_str(account_key);
    const char *c_sas = sexp_str(sas_token);
    double c_cache = Rf_asReal(cache_size_mb);

    AzureBackendData *az = azure_backend_create(c_url, c_acc, c_key, c_sas);
    if (!az)
        Rf_error("Failed to create Azure backend for '%s'", c_url);

    long long max_cache = (long long)(c_cache * 1024 * 1024);
    CloudStream *cs = cloud_stream_create(c_url,
        &azure_backend_vtable, az, CLOUD_BLOCK_SIZE, max_cache);
    if (!cs)
    {
        azure_backend_vtable.close(az);
        Rf_error("Failed to create cloud stream for '%s'", c_url);
    }

    PdGDSFile file = GDS_File_Open_Callback(
        (TdCbStreamRead)gdscloud_cb_read,
        (TdCbStreamWrite)NULL,
        (TdCbStreamSeek)gdscloud_cb_seek,
        (TdCbStreamGetSize)gdscloud_cb_getsize,
        (TdCbStreamSetSize)NULL,
        (TdCbStreamClose)gdscloud_cb_close,
        cs, TRUE, FALSE);

    if (!file)
    {
        cloud_stream_close(cs);
        Rf_error("Failed to open GDS file from '%s'", c_url);
    }

    return make_gds_class(file, c_url);
}


// =====================================================================
// .Call: Clear cache (placeholder - individual stream caches are
//        freed when streams close; this is a no-op for now)
// =====================================================================

SEXP gdscloud_cache_clear(void)
{
    // Individual stream caches are managed per-stream.
    // This function could maintain a global registry in the future.
    return R_NilValue;
}


// =====================================================================
// .Call: Cache info
// =====================================================================

SEXP gdscloud_cache_info(void)
{
    // Return a basic list with global cache size setting
    SEXP ans = PROTECT(Rf_allocVector(VECSXP, 2));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, 2));
    SET_STRING_ELT(names, 0, Rf_mkChar("hits"));
    SET_STRING_ELT(names, 1, Rf_mkChar("misses"));
    Rf_setAttrib(ans, R_NamesSymbol, names);

    // Placeholder - would need a global registry to aggregate
    SET_VECTOR_ELT(ans, 0, Rf_ScalarInteger(0));
    SET_VECTOR_ELT(ans, 1, Rf_ScalarInteger(0));

    UNPROTECT(2);
    return ans;
}
