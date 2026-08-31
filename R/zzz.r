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

    # register the URL scheme handlers with gdsfmt, so that openfn.gds()
    # dispatches cloud URLs to this package without gdsfmt needing a
    # reverse dependency on gdscloud
    gdsRegisterCloudHandler("s3",    .open_s3,    pkgname)
    gdsRegisterCloudHandler("gs",    .open_gcs,   pkgname)
    gdsRegisterCloudHandler("az",    .open_azure, pkgname)
    gdsRegisterCloudHandler("http",  .open_http,  pkgname)
    gdsRegisterCloudHandler("https", .open_http,  pkgname)
}


.onUnload <- function(libpath)
{
    # unregister the cloud handlers from gdsfmt
    gdsUnregisterCloudHandler(c("s3", "gs", "az", "http", "https"))
    library.dynam.unload("gdscloud", libpath)
}
