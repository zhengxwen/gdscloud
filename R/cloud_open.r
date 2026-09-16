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
# Get or set the package options (timeouts, cache size)
#
gdsCloudOptions <- function(connect_timeout=NULL, timeout=NULL,
    cache_size=NULL)
{
    opt <- .gdscloud_env$options
    check_pos <- function(v, name)
    {
        if (!is.numeric(v) || length(v) != 1L || is.na(v) || v <= 0)
            stop("'", name, "' must be a single positive number.", call.=FALSE)
        as.numeric(v)
    }
    if (!is.null(connect_timeout))
        opt$connect_timeout <- check_pos(connect_timeout, "connect_timeout")
    if (!is.null(timeout))
        opt$timeout <- check_pos(timeout, "timeout")
    if (!is.null(cache_size))
    {
        opt$cache_size <- check_pos(cache_size, "cache_size")
        .gdscloud_env$cache_size_mb <- opt$cache_size
    }
    .gdscloud_env$options <- opt
    if (is.null(connect_timeout) && is.null(timeout) && is.null(cache_size))
        opt
    else
        invisible(opt)
}


#############################################################
# Internal: initialize the options at load time. Defaults can be
# overridden persistently via R options ('gdscloud.<name>') or the
# environment variables GDSCLOUD_<NAME>, e.g. in .Rprofile / .Renviron
#
.init_options <- function()
{
    get_num <- function(name, default)
    {
        raw <- getOption(paste0("gdscloud.", name),
            Sys.getenv(paste0("GDSCLOUD_", toupper(name)), ""))
        v <- suppressWarnings(as.numeric(raw))
        if (length(v) == 1L && !is.na(v) && v > 0) v else default
    }
    .gdscloud_env$options <- list(
        connect_timeout = get_num("connect_timeout", 30),
        timeout         = get_num("timeout", 60),
        cache_size      = get_num("cache_size_mb", 64)
    )
    .gdscloud_env$cache_size_mb <- .gdscloud_env$options$cache_size
    invisible()
}


#############################################################
# Internal: push the transfer timeouts (seconds) to the C code
#
.apply_timeouts <- function()
{
    opt <- .gdscloud_env$options
    .Call(gdscloud_set_timeouts, opt$connect_timeout, opt$timeout)
    invisible()
}


#############################################################
# Internal: open from HTTP/HTTPS
#
.open_http <- function(url, ...)
{
    .apply_timeouts()
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
    .apply_timeouts()
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
    .apply_timeouts()
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
    .apply_timeouts()
    cred <- .get_azure_credentials(url)
    cache_mb <- .gdscloud_env$cache_size_mb
    # Call C function with credentials and cache size
    .Call(gdscloud_open_azure, url,
        cred$account_name, cred$account_key, cred$sas_token, cache_mb)
}
