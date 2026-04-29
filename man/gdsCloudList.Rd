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
\dontrun{
gds1 <- gdsCloudOpen("s3://bucket/file1.gds")
gds2 <- gdsCloudOpen("gs://bucket/file2.gds")

# List all open cloud streams
gdsCloudList()
##                          url file_size cache_blocks cache_hits cache_misses
## 1    s3://bucket/file1.gds  86500000           12         50            12
## 2    gs://bucket/file2.gds  42000000            8         30             8

closefn.gds(gds1)
closefn.gds(gds2)
}
}
\keyword{GDS}
\keyword{cloud}
