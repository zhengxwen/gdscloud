\name{gdsCloudOpen}
\alias{gdsCloudOpen}
\title{Open a GDS File from Cloud Storage}
\description{
    Opens a read-only GDS (Genomic Data Structure) file from cloud storage
    services including Amazon S3, Google Cloud Storage, and Azure Blob
    Storage, or from any HTTP/HTTPS URL.
}
\usage{
gdsCloudOpen(url, allow.error=FALSE)
}
\arguments{
    \item{url}{a character string specifying the cloud URL, e.g.,
        \code{"s3://bucket/key"}, \code{"gs://bucket/key"},
        \code{"az://container/blob"}, or
        \code{"https://example.com/path/to/file.gds"}}
    \item{allow.error}{if \code{TRUE}, return the GDS file object even if
        there are errors in the file structure}
}
\value{
    An object of class \code{gds.class}, identical to the return value
    of \code{\link[gdsfmt]{openfn.gds}}.
}
\details{
    The function automatically detects the URL scheme and uses the
    appropriate cloud backend. Authentication credentials are read from
    environment variables or from values set via \code{\link{gdsCloudConfigS3}},
    \code{\link{gdsCloudConfigHTTP}}, etc.

    All cloud access is read-only. The file data is cached in memory using
    an LRU block cache (default 64 MB) for efficient random access.

    When the \code{gdscloud} package is loaded, \code{openfn.gds()} from
    the \code{gdsfmt} package automatically detects cloud URLs and delegates
    to this function.
}
\seealso{
    \code{\link{gdsCloudConfigS3}}, \code{\link{gdsCloudConfigHTTP}},
    \code{\link{gdsCloudCacheSize}},
    \code{\link[gdsfmt]{openfn.gds}}, \code{\link[gdsfmt]{closefn.gds}}
}
\examples{
\dontrun{
# Set AWS credentials
gdsCloudConfigS3(
    aws_access_key_id = "AKIAIOSFODNN7EXAMPLE",
    aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    region = "us-east-1"
)

# Open from S3
gds <- gdsCloudOpen("s3://my-bucket/data/example.gds")
read.gdsn(index.gdsn(gds, "genotype"))
closefn.gds(gds)

# Or transparently via openfn.gds (when gdscloud is loaded)
gds <- openfn.gds("s3://my-bucket/data/example.gds")
closefn.gds(gds)

# Open from an HTTP/HTTPS URL (public, no auth needed)
gds <- gdsCloudOpen("https://example.com/data/example.gds")
closefn.gds(gds)

# Authenticated HTTP endpoint
gdsCloudConfigHTTP(bearer_token = "my-secret-token")
gds <- gdsCloudOpen("https://private.example.com/data/example.gds")
closefn.gds(gds)
}
}
\keyword{GDS}
\keyword{cloud}
