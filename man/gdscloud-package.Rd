\name{gdscloud-package}
\alias{gdscloud-package}
\alias{gdscloud}
\docType{package}
\title{Cloud Storage Access for GDS Files}
\description{
    \pkg{gdscloud} extends the \pkg{gdsfmt} package to provide transparent,
    read-only access to GDS (Genomic Data Structure) files stored on cloud
    storage services -- Amazon S3 (\code{s3://}), Google Cloud Storage
    (\code{gs://}), and Azure Blob Storage (\code{az://}) -- as well as any
    HTTP/HTTPS URL. It uses HTTP Range requests via \code{libcurl} together
    with an in-memory LRU block cache to minimize network overhead.

    Once the package is loaded, \code{\link[gdsfmt]{openfn.gds}} automatically
    recognizes cloud URLs and opens them transparently.
}
\details{
    Main functions:
    \itemize{
        \item \code{\link{gdsCloudOpen}}: open a GDS file from a cloud URL.
        \item \code{\link{gdsCloudSchemes}}: list the supported URL schemes.
        \item \code{\link{gdsCloudConfigS3}}, \code{\link{gdsCloudConfigGCS}},
            \code{\link{gdsCloudConfigAzure}},
            \code{\link{gdsCloudConfigHTTP}}: configure credentials.
        \item \code{\link{gdsCloudExportCredentials}}: propagate credentials
            to parallel workers.
        \item \code{\link{gdsCloudCacheSize}}, \code{\link{gdsCloudList}}:
            control the block cache and inspect open cloud streams.
    }
}
\author{
    Xiuwen Zheng \email{zhengx@u.washington.edu}
}
\seealso{
    \code{\link{gdsCloudOpen}}, \code{\link[gdsfmt]{openfn.gds}}
}
\examples{
# list the supported cloud URL schemes
gdsCloudSchemes()
}
\keyword{package}
