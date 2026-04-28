# ===========================================================================
#
# zzz.r: Package initialization and handler registration
#
# Copyright (C) 2026    Xiuwen Zheng
#
# This file is part of gdscloud.
# LGPL-3 License
# ===========================================================================


# Internal package environment for configuration and state
.gdscloud_env <- new.env(parent=emptyenv())

.onLoad <- function(libname, pkgname)
{
    # default settings
    .gdscloud_env$cache_size_mb <- 64

    # register URL scheme handlers with gdsfmt
    if (requireNamespace("gdsfmt", quietly=TRUE))
    {
        reg_fn <- get(".gds_register_cloud_handler",
            envir=asNamespace("gdsfmt"), inherits=FALSE)
        if (is.function(reg_fn))
        {
            reg_fn("s3",  function(url, ...) .open_s3(url, ...))
            reg_fn("gs",  function(url, ...) .open_gcs(url, ...))
            reg_fn("az",  function(url, ...) .open_azure(url, ...))
        }
    }
}

.onUnload <- function(libpath)
{
    library.dynam.unload("gdscloud", libpath)
}
