# ===========================================================================
#
# cache.r: Cache control functions
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
gdsCloudCacheInfo <- function(verbose=TRUE)
{
    info <- .Call(gdscloud_cache_info)
    if (isTRUE(verbose))
    {
        cat("gdscloud cache settings:\n")
        cat("  Default cache size:", .gdscloud_env$cache_size_mb, "MB\n")
        cat("  Block size: 1 MB\n")
        if (!is.null(info))
        {
            cat("  Open cloud streams:", info$num_streams, "\n")
            cat("  Global cache hits:", info$hits, "\n")
            cat("  Global cache misses:", info$misses, "\n")
        }
    }
    invisible(info)
}


#############################################################
# List all open cloud streams
#
gdsCloudList <- function()
{
    .Call(gdscloud_list_streams)
}
