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
    # timeouts and cache size (see ?gdsCloudOptions)
    .init_options()

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
