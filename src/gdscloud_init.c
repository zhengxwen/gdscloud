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


// External declarations from gdscloud.c
extern SEXP gdscloud_open_s3(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP gdscloud_open_gcs(SEXP, SEXP, SEXP);
extern SEXP gdscloud_open_azure(SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP gdscloud_cache_clear(void);
extern SEXP gdscloud_cache_info(void);


// .Call method table
static const R_CallMethodDef CallEntries[] = {
    {"gdscloud_open_s3",     (DL_FUNC) &gdscloud_open_s3,     6},
    {"gdscloud_open_gcs",    (DL_FUNC) &gdscloud_open_gcs,    3},
    {"gdscloud_open_azure",  (DL_FUNC) &gdscloud_open_azure,  5},
    {"gdscloud_cache_clear", (DL_FUNC) &gdscloud_cache_clear,  0},
    {"gdscloud_cache_info",  (DL_FUNC) &gdscloud_cache_info,   0},
    {NULL, NULL, 0}
};


void R_init_gdscloud(DllInfo *info)
{
    // register .Call methods
    R_registerRoutines(info, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(info, FALSE);
}
