\name{gds.cloud.config.s3}
\alias{gds.cloud.config.s3}
\alias{gds.cloud.config.gcs}
\alias{gds.cloud.config.azure}
\title{Configure Cloud Storage Credentials}
\description{
    Set authentication credentials for cloud storage access.
    Credentials set via these functions take priority over environment
    variables.
}
\usage{
gds.cloud.config.s3(aws_access_key_id=NULL, aws_secret_access_key=NULL,
    region=NULL, session_token=NULL)

gds.cloud.config.gcs(access_token=NULL)

gds.cloud.config.azure(account_name=NULL, account_key=NULL, sas_token=NULL)
}
\arguments{
    \item{aws_access_key_id}{AWS access key ID}
    \item{aws_secret_access_key}{AWS secret access key}
    \item{region}{AWS region (default: \code{"us-east-1"})}
    \item{session_token}{AWS session token for temporary credentials}
    \item{access_token}{Google Cloud OAuth2 access token}
    \item{account_name}{Azure storage account name}
    \item{account_key}{Azure storage account key}
    \item{sas_token}{Azure SAS (Shared Access Signature) token}
}
\details{
    If not set explicitly, credentials fall back to environment variables:
    \itemize{
        \item S3: \code{AWS_ACCESS_KEY_ID}, \code{AWS_SECRET_ACCESS_KEY},
            \code{AWS_DEFAULT_REGION}, \code{AWS_SESSION_TOKEN}
        \item GCS: \code{GCS_ACCESS_TOKEN}
        \item Azure: \code{AZURE_STORAGE_ACCOUNT}, \code{AZURE_STORAGE_KEY},
            \code{AZURE_STORAGE_SAS_TOKEN}
    }
}
\value{
    None (invisible).
}
\seealso{
    \code{\link{cloud.open.gds}}
}
\examples{
\dontrun{
# AWS S3
gds.cloud.config.s3(
    aws_access_key_id = "your_key",
    aws_secret_access_key = "your_secret",
    region = "us-east-1"
)

# Google Cloud Storage
gds.cloud.config.gcs(access_token = "ya29.example_token")

# Azure Blob Storage
gds.cloud.config.azure(
    account_name = "mystorageaccount",
    account_key = "base64encodedkey=="
)
}
}
\keyword{GDS}
\keyword{cloud}
