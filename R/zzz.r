# ===========================================================================
#
# zzz.r: Package initialization and handler registration
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


# Internal package environment for configuration and state
.gdscloud_env <- new.env(parent=emptyenv())

.onLoad <- function(libname, pkgname)
{
    # default cache size (MB); overridable via the option
    # 'gdscloud.cache_size_mb' or the environment variable
    # GDSCLOUD_CACHE_SIZE_MB so it can persist across sessions
    # (e.g. in .Rprofile / .Renviron)
    raw <- getOption("gdscloud.cache_size_mb",
        Sys.getenv("GDSCLOUD_CACHE_SIZE_MB", "64"))
    sz <- if (is.numeric(raw)) raw
        else if (grepl("^[0-9.]+$", raw)) as.numeric(raw) else NA_real_
    .gdscloud_env$cache_size_mb <-
        if (length(sz) == 1L && !is.na(sz) && sz > 0) sz else 64L

    # registry of URL-specific credential entries (longest-prefix match)
    .gdscloud_env$url_credentials <- list()

    # register URL scheme handlers with gdsfmt. The registration hooks
    # .gds_register_cloud_handler() / .gds_unregister_cloud_handler() are
    # internal (non-exported) gdsfmt functions, so they are reached via
    # asNamespace("gdsfmt"); this lets openfn.gds() dispatch cloud URLs to
    # gdscloud without gdsfmt depending on this package.
    if (requireNamespace("gdsfmt", quietly=TRUE))
    {
        # register handlers for s3://, gs://, az:// URLs; access defensively
        # so a gdsfmt without the hook cannot abort package loading
        reg_fn <- tryCatch(
            get(".gds_register_cloud_handler",
                envir=asNamespace("gdsfmt"), inherits=FALSE),
            error=function(e) NULL)
        if (is.function(reg_fn))
        {
            reg_fn("s3",    function(url, ...) .open_s3(url, ...), pkgname)
            reg_fn("gs",    function(url, ...) .open_gcs(url, ...), pkgname)
            reg_fn("az",    function(url, ...) .open_azure(url, ...), pkgname)
            reg_fn("http",  function(url, ...) .open_http(url, ...), pkgname)
            reg_fn("https", function(url, ...) .open_http(url, ...), pkgname)
        }
    }
}

.onUnload <- function(libpath)
{
    # unregister cloud handlers from gdsfmt
    if (requireNamespace("gdsfmt", quietly=TRUE))
    {
        unreg_fn <- tryCatch(
            get(".gds_unregister_cloud_handler",
                envir=asNamespace("gdsfmt"), inherits=FALSE),
            error=function(e) NULL)
        if (is.function(unreg_fn))
        {
            unreg_fn("s3")
            unreg_fn("gs")
            unreg_fn("az")
            unreg_fn("http")
            unreg_fn("https")
        }
    }
    library.dynam.unload("gdscloud", libpath)
}
