\name{gds.cloud.cache_size}
\alias{gds.cloud.cache_size}
\alias{gds.cloud.cache_clear}
\alias{gds.cloud.info}
\title{Cache Control for Cloud GDS Access}
\description{
    Manage the block cache used for cloud storage access.
}
\usage{
gds.cloud.cache_size(size_mb=64)
gds.cloud.cache_clear()
gds.cloud.info()
}
\arguments{
    \item{size_mb}{default cache size in megabytes for new cloud streams}
}
\details{
    Cloud GDS access uses an LRU block cache (1 MB blocks) to minimize
    HTTP requests. The default cache size is 64 MB per stream.

    \code{gds.cloud.cache_size} sets the default cache size for
    subsequently opened streams.

    \code{gds.cloud.cache_clear} frees cached data.

    \code{gds.cloud.info} prints cache statistics.
}
\value{
    \code{gds.cloud.cache_size}: the new cache size (invisible).
    \code{gds.cloud.cache_clear}: none (invisible).
    \code{gds.cloud.info}: a list with cache statistics (invisible).
}
\seealso{
    \code{\link{cloud.open.gds}}
}
\examples{
\dontrun{
# Increase cache to 128 MB
gds.cloud.cache_size(128)

# Show cache info
gds.cloud.info()
}
}
\keyword{GDS}
\keyword{cloud}
