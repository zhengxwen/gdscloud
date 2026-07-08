# ===========================================================================
#
# cloud_open.r: Open GDS files from cloud storage
#
# Copyright (C) 2026    Xiuwen Zheng
#
# This is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License Version 3 as
# published by the Free Software Foundation.
#
# gdscloud is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with gdscloud.
# If not, see <http://www.gnu.org/licenses/>.


#############################################################
# Open a GDS file from a cloud URL
#
gdsCloudOpen <- function(url)
{
    stopifnot(is.character(url), length(url)==1L)

    # parse the URL scheme
    scheme <- sub("://.*", "", url)
    supported_schemes <- names(gdsCloudSchemes())
    if (!scheme %in% supported_schemes)
    {
        schemes_txt <- toString(paste0(supported_schemes, "://"))
        stop("Unsupported URL scheme: '", scheme, "'. ",
             "Supported schemes: ", schemes_txt)
    }

    # dispatch to the appropriate backend
    ans <- switch(scheme,
        "http"  = .open_http(url),
        "https" = .open_http(url),
        "s3"    = .open_s3(url),
        "gs"    = .open_gcs(url),
        "az"    = .open_azure(url)
    )

    ans
}


#############################################################
# List supported cloud URL schemes
#
gdsCloudSchemes <- function()
{
    c(
        http  = "HTTP",
        https = "HTTPS",
        s3    = "Amazon S3",
        gs    = "Google Cloud Storage",
        az    = "Azure Blob Storage"
    )
}


#############################################################
# Internal: open from HTTP/HTTPS
#
.open_http <- function(url, ...)
{
    cred <- .get_http_credentials(url)
    cache_mb <- .gdscloud_env$cache_size_mb
    # Build auth header string (e.g. "Bearer <token>")
    auth_header <- ""
    if (nzchar(cred$bearer_token))
        auth_header <- paste("Bearer", cred$bearer_token)
    # Call C function with auth header and cache size
    .Call(gdscloud_open_http, url, auth_header, cache_mb)
}


#############################################################
# Internal: open from S3
#
.open_s3 <- function(url, ...)
{
    # get credentials (URL-specific entry takes priority)
    cred <- .get_s3_credentials(url)
    cache_mb <- .gdscloud_env$cache_size_mb
    # Call C function with credentials and cache size
    .Call(gdscloud_open_s3, url,
        cred$access_key, cred$secret_key, cred$region, cred$session_token,
        cache_mb)
}


#############################################################
# Internal: open from GCS
#
.open_gcs <- function(url, ...)
{
    cred <- .get_gcs_credentials(url)
    cache_mb <- .gdscloud_env$cache_size_mb
    # Call C function with credentials and cache size
    .Call(gdscloud_open_gcs, url, cred$access_token, cache_mb)
}


#############################################################
# Internal: open from Azure
#
.open_azure <- function(url, ...)
{
    cred <- .get_azure_credentials(url)
    cache_mb <- .gdscloud_env$cache_size_mb
    # Call C function with credentials and cache size
    .Call(gdscloud_open_azure, url,
        cred$account_name, cred$account_key, cred$sas_token, cache_mb)
}
