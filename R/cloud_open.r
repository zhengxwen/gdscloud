# ===========================================================================
#
# cloud_open.r: Open GDS files from cloud storage
#
# Copyright (C) 2026    Xiuwen Zheng
#
# This file is part of gdscloud.
# LGPL-3 License
# ===========================================================================


#############################################################
# Open a GDS file from a cloud URL
#
gdsCloudOpen <- function(url, allow.error=FALSE)
{
    stopifnot(is.character(url), length(url)==1L)
    stopifnot(is.logical(allow.error), length(allow.error)==1L)

    # parse the URL scheme
    scheme <- sub("://.*", "", url)
    if (!scheme %in% c("s3", "gs", "az"))
        stop("Unsupported URL scheme: '", scheme, "'. ",
             "Supported schemes: s3://, gs://, az://")

    # dispatch to the appropriate backend
    ans <- switch(scheme,
        "s3"  = .open_s3(url, allow.error),
        "gs"  = .open_gcs(url, allow.error),
        "az"  = .open_azure(url, allow.error)
    )

    ans
}


#############################################################
# Internal: open from S3
#
.open_s3 <- function(url, allow.error=FALSE)
{
    # get credentials
    cred <- .get_s3_credentials()
    cache_mb <- .gdscloud_env$cache_size_mb
    # Call C function with credentials and cache size
    .Call(gdscloud_open_s3, url,
        cred$access_key, cred$secret_key,
        cred$region, cred$session_token,
        cache_mb)
}


#############################################################
# Internal: open from GCS
#
.open_gcs <- function(url, allow.error=FALSE)
{
    cred <- .get_gcs_credentials()
    cache_mb <- .gdscloud_env$cache_size_mb
    # Call C function with credentials and cache size
    .Call(gdscloud_open_gcs, url,
        cred$access_token,
        cache_mb)
}


#############################################################
# Internal: open from Azure
#
.open_azure <- function(url, allow.error=FALSE)
{
    cred <- .get_azure_credentials()
    cache_mb <- .gdscloud_env$cache_size_mb
    # Call C function with credentials and cache size
    .Call(gdscloud_open_azure, url,
        cred$account_name, cred$account_key,
        cred$sas_token,
        cache_mb)
}
