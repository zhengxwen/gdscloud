\name{gdsCloudCacheSize}
\alias{gdsCloudCacheSize}
\alias{gdsCloudCacheClear}
\alias{gdsCloudInfo}
\title{Cache Control for Cloud GDS Access}
\description{
    Manage the block cache used for cloud storage access.
}
\usage{
gdsCloudCacheSize(size_mb=64)
gdsCloudCacheClear()
gdsCloudInfo()
}
\arguments{
    \item{size_mb}{default cache size in megabytes for new cloud streams}
}
\details{
    Cloud GDS access uses an LRU block cache (1 MB blocks) to minimize
    HTTP requests. The default cache size is 64 MB per stream.

    \code{gdsCloudCacheSize} sets the default cache size for
    subsequently opened streams.

    \code{gdsCloudCacheClear} frees cached data.

    \code{gdsCloudInfo} prints cache statistics.
}
\value{
    \code{gdsCloudCacheSize}: the new cache size (invisible).
    \code{gdsCloudCacheClear}: none (invisible).
    \code{gdsCloudInfo}: a list with cache statistics (invisible).
}
\seealso{
    \code{\link{gdsCloudOpen}}
}
\examples{
\dontrun{
# Increase cache to 128 MB
gdsCloudCacheSize(128)

# Show cache info
gdsCloudInfo()
}
}
\keyword{GDS}
\keyword{cloud}
