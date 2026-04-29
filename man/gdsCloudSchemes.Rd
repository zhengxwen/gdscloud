\name{gdsCloudSchemes}
\alias{gdsCloudSchemes}
\title{List Supported Cloud URL Schemes}
\description{
    Returns the cloud URL schemes supported by gdscloud, with their
    corresponding storage service names.
}
\usage{
gdsCloudSchemes()
}
\value{
    A named character vector where names are the URL scheme prefixes
    (\code{"s3"}, \code{"gs"}, \code{"az"}) and values are the
    storage service descriptions.
}
\seealso{
    \code{\link{gdsCloudOpen}}
}
\examples{
gdsCloudSchemes()
##                    s3                   gs                   az
##          "Amazon S3" "Google Cloud Storage"  "Azure Blob Storage"
}
\keyword{GDS}
\keyword{cloud}
