// ===========================================================
// gdscloud: Cloud Storage Access for GDS Files
//
// gdscloud_init.c: Package initialization and .Call registration
//
// Copyright (C) 2026    Xiuwen Zheng
//
// This file is part of gdscloud.
// LGPL-3 License
// ===========================================================

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <curl/curl.h>

#include "cloud_stream.h"


// External declarations from gdscloud.c
extern SEXP gdscloud_open_s3(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP gdscloud_open_gcs(SEXP, SEXP, SEXP);
extern SEXP gdscloud_open_azure(SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP gdscloud_cache_clear(void);
extern SEXP gdscloud_cache_info(void);
extern SEXP gdscloud_list_streams(void);


// .Call method table
static const R_CallMethodDef CallEntries[] = {
	{ "gdscloud_open_s3",     (DL_FUNC) &gdscloud_open_s3,     6 },
	{ "gdscloud_open_gcs",    (DL_FUNC) &gdscloud_open_gcs,    3 },
	{ "gdscloud_open_azure",  (DL_FUNC) &gdscloud_open_azure,  5 },
	{ "gdscloud_cache_clear", (DL_FUNC) &gdscloud_cache_clear,  0 },
	{ "gdscloud_cache_info",  (DL_FUNC) &gdscloud_cache_info,   0 },
	{ "gdscloud_list_streams",(DL_FUNC) &gdscloud_list_streams, 0 },
	{ NULL, NULL, 0 }
};


// defined in R_GDS2.h (included via R_gdscloud.c)
extern void Init_GDS_Routines(void);

void R_init_gdscloud(DllInfo *info)
{
	// register .Call methods
	R_registerRoutines(info, NULL, CallEntries, NULL, NULL);
	R_useDynamicSymbols(info, FALSE);

	// initialize gdsfmt function pointers (required before any GDS_* call)
	Init_GDS_Routines();

	// initialize libcurl (required before any curl_easy_* call)
	curl_global_init(CURL_GLOBAL_ALL);

	// initialize fork tracking (required for mclapply/forked processes)
	cloud_init_fork_tracking();
}

void R_unload_gdscloud(DllInfo *info)
{
	curl_global_cleanup();
}
