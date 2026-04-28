# ===========================================================================
#
# cache.r: Cache control functions
#
# Copyright (C) 2026    Xiuwen Zheng
#
# This file is part of gdscloud.
# LGPL-3 License
# ===========================================================================


#############################################################
# Set the default cache size for new cloud streams (in MB)
#
gdsCloudCacheSize <- function(size_mb=64)
{
    stopifnot(is.numeric(size_mb), length(size_mb)==1L, size_mb > 0)
    .gdscloud_env$cache_size_mb <- size_mb
    invisible(size_mb)
}


#############################################################
# Clear all internal caches
#
gdsCloudCacheClear <- function()
{
    .Call(gdscloud_cache_clear)
    invisible()
}


#############################################################
# Show cache information and statistics
#
gdsCloudInfo <- function()
{
    info <- .Call(gdscloud_cache_info)
    cat("gdscloud cache settings:\n")
    cat("  Default cache size:", .gdscloud_env$cache_size_mb, "MB\n")
    cat("  Block size: 1 MB\n")
    if (!is.null(info))
    {
        cat("  Global cache hits:", info$hits, "\n")
        cat("  Global cache misses:", info$misses, "\n")
    }
    invisible(info)
}
