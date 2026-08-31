\name{gdsCloudList}
\alias{gdsCloudList}
\title{List Open Cloud Streams}
\description{
    List all currently open cloud GDS file streams with per-stream
    details including URL, file size, and cache statistics.
}
\usage{
gdsCloudList()
}
\value{
    A data frame with one row per open cloud stream and columns:
    \item{url}{the cloud URL (e.g., \code{"s3://bucket/key"})}
    \item{file_size}{total file size in bytes}
    \item{cache_blocks}{number of cached blocks currently held}
    \item{cache_hits}{cumulative cache hit count}
    \item{cache_misses}{cumulative cache miss count}
    If no streams are open, an empty data frame is returned.
}
\seealso{
    \code{\link{gdsCloudOpen}}, \code{\link{gdsCloudCacheInfo}}
}
\examples{
# List currently open cloud streams (empty if none open)
gdsCloudList()

\donttest{
gds <- gdsCloudOpen("s3://gds-stat/download/hapmap/hapmap_r23a.gds")
gdsCloudList()
closefn.gds(gds)
}
}
\keyword{GDS}
\keyword{cloud}
