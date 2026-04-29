\name{gdsCloudCacheSize}
\alias{gdsCloudCacheSize}
\alias{gdsCloudCacheClear}
\alias{gdsCloudCacheInfo}
\title{Cache Control for Cloud GDS Access}
\description{
    Manage the block cache used for cloud storage access.
}
\usage{
gdsCloudCacheSize(size_mb=64)
gdsCloudCacheClear()
gdsCloudCacheInfo(verbose=TRUE)
}
\arguments{
    \item{size_mb}{default cache size in megabytes for new cloud streams}
    \item{verbose}{if \code{TRUE}, print cache statistics to the console}
}
\details{
    Cloud GDS access uses an LRU block cache (1 MB blocks) to minimize
    HTTP requests. The default cache size is 64 MB per stream.

    \code{gdsCloudCacheSize} sets the default cache size for
    subsequently opened streams.

    \code{gdsCloudCacheClear} frees cached data.

    \code{gdsCloudCacheInfo} prints cache statistics.
}
\value{
    \code{gdsCloudCacheSize}: the new cache size (invisible).
    \code{gdsCloudCacheClear}: none (invisible).
    \code{gdsCloudCacheInfo}: a list with cache statistics (invisible).
}
\seealso{
    \code{\link{gdsCloudOpen}}
}
\examples{
\dontrun{
# Increase cache to 128 MB
gdsCloudCacheSize(128)

# Show cache info
gdsCloudCacheInfo()
}
}
\keyword{GDS}
\keyword{cloud}
