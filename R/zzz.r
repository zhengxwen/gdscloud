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
    # default settings
    .gdscloud_env$cache_size_mb <- 64L

    # registry of URL-specific credential entries (longest-prefix match)
    .gdscloud_env$url_credentials <- list()

    # register URL scheme handlers with gdsfmt
    if (requireNamespace("gdsfmt", quietly=TRUE))
    {
        # register handlers for s3://, gs://, az:// URLs
        reg_fn <- get(".gds_register_cloud_handler",
            envir=asNamespace("gdsfmt"), inherits=FALSE)
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
