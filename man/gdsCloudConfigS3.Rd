\name{gdsCloudConfigS3}
\alias{gdsCloudConfigS3}
\alias{gdsCloudConfigGCS}
\alias{gdsCloudConfigAzure}
\title{Configure Cloud Storage Credentials}
\description{
    Set authentication credentials for cloud storage access.
    Credentials set via these functions take priority over environment
    variables. URL-specific credentials can also be registered so that
    different URLs (e.g. different buckets) use different keys.
}
\usage{
gdsCloudConfigS3(aws_access_key_id=NULL, aws_secret_access_key=NULL,
    region=NULL, session_token=NULL, endpoint=NULL, path_style=NULL,
    url=NULL)

gdsCloudConfigGCS(access_token=NULL, url=NULL)

gdsCloudConfigAzure(account_name=NULL, account_key=NULL, sas_token=NULL,
    url=NULL)
}
\arguments{
    \item{aws_access_key_id}{AWS access key ID}
    \item{aws_secret_access_key}{AWS secret access key}
    \item{region}{AWS region (default: \code{"us-east-1"})}
    \item{session_token}{AWS session token for temporary credentials}
    \item{endpoint}{base URL of an S3-compatible service, e.g.
        \code{"https://minio.example.org:9000"} or
        \code{"https://<account>.r2.cloudflarestorage.com"}; \code{""}
        or unset selects Amazon S3 (\code{https://<bucket>.s3.<region>.amazonaws.com}).
        The scheme defaults to \code{https://} when omitted; a path
        prefix is allowed, e.g. \code{"https://host/s3"}.}
    \item{path_style}{\code{TRUE} to address objects as
        \code{https://<host>/<bucket>/<key>} (path style), \code{FALSE}
        for \code{https://<bucket>.<host>/<key>} (virtual-hosted style),
        \code{NA} (the default) for path style with a custom
        \code{endpoint} and virtual-hosted style with Amazon S3}
    \item{access_token}{Google Cloud OAuth2 access token}
    \item{account_name}{Azure storage account name}
    \item{account_key}{Azure storage account key}
    \item{sas_token}{Azure SAS (Shared Access Signature) token}
    \item{url}{optional URL prefix (e.g. \code{"s3://my-bucket/"}) to
        associate the given credentials with. When \code{NULL} (the
        default) the credentials are stored as the global defaults.
        Passing \code{url} with all credential arguments \code{NULL}
        removes the entry for that prefix. The URL scheme must match
        the function (\code{s3://} for \code{gdsCloudConfigS3},
        \code{gs://} for \code{gdsCloudConfigGCS}, \code{az://} for
        \code{gdsCloudConfigAzure})}
}
\details{
    If not set explicitly, credentials fall back to environment variables:
    \itemize{
        \item S3: \code{AWS_ACCESS_KEY_ID}, \code{AWS_SECRET_ACCESS_KEY},
            \code{AWS_DEFAULT_REGION}, \code{AWS_SESSION_TOKEN};
            \code{AWS_ENDPOINT_URL_S3} or \code{AWS_ENDPOINT_URL} for
            \code{endpoint}, \code{GDSCLOUD_S3_PATH_STYLE}
            (\code{true}/\code{false}) for \code{path_style}
        \item GCS: \code{GCS_ACCESS_TOKEN}
        \item Azure: \code{AZURE_STORAGE_ACCOUNT}, \code{AZURE_STORAGE_KEY},
            \code{AZURE_STORAGE_SAS_TOKEN}
    }

    When a URL is opened via \code{\link{gdsCloudOpen}}, credentials
    are resolved in the following order (the first non-empty value
    wins for each field):
    \enumerate{
        \item the URL-specific entry whose registered prefix is the
            longest prefix of the opened URL (within the same scheme);
        \item the global values set by these functions with
            \code{url=NULL};
        \item the corresponding environment variable.
    }
    Registered URL prefixes are normalized by appending a trailing
    \code{"/"} if missing, so \code{"s3://bucket"} and
    \code{"s3://bucket/"} behave identically.

    \subsection{S3-compatible services}{
    Any service that implements the S3 API with AWS Signature Version 4
    can be used through \code{endpoint}: MinIO, Ceph RGW, Cloudflare R2,
    Wasabi, Backblaze B2, DigitalOcean Spaces, and the S3 interfaces of
    Alibaba OSS, Tencent COS, Oracle OCI and IBM COS, among others.
    \code{s3://bucket/key} then refers to the bucket on that service.
    Most of these expect path-style addressing, which is the default for
    a custom endpoint; \code{region} is still part of the signature and
    should be set to whatever the service documents (\code{"us-east-1"}
    or \code{"auto"} is accepted by most). A bucket name containing dots
    on Amazon S3 needs \code{path_style=TRUE}, because virtual-hosted
    HTTPS would fail the certificate check.
    }
}
\value{
    None (invisible).
}
\seealso{
    \code{\link{gdsCloudOpen}}
}
\examples{
# AWS S3
gdsCloudConfigS3(
    aws_access_key_id = "your_key",
    aws_secret_access_key = "your_secret",
    region = "us-east-1"
)

# Google Cloud Storage
gdsCloudConfigGCS(access_token = "ya29.example_token")

# Azure Blob Storage
gdsCloudConfigAzure(
    account_name = "mystorageaccount",
    account_key = "base64encodedkey=="
)

\dontrun{
# URL-specific credentials: different keys for different buckets
gdsCloudConfigS3(
    aws_access_key_id = "KEY_A",
    aws_secret_access_key = "SECRET_A",
    url = "s3://bucket-a/"
)
gdsCloudConfigS3(
    aws_access_key_id = "KEY_B",
    aws_secret_access_key = "SECRET_B",
    url = "s3://bucket-b/"
)
# Remove a previously registered URL-specific entry
gdsCloudConfigS3(url = "s3://bucket-a/")

# An S3-compatible service (MinIO on a local network)
gdsCloudConfigS3(
    aws_access_key_id = "minioadmin",
    aws_secret_access_key = "minioadmin",
    endpoint = "http://minio.lab.internal:9000"
)
gds <- gdsCloudOpen("s3://genomics/hapmap.gds")
closefn.gds(gds)

# Cloudflare R2, scoped to one bucket
gdsCloudConfigS3(
    aws_access_key_id = "R2_KEY",
    aws_secret_access_key = "R2_SECRET",
    region = "auto",
    endpoint = "https://<ACCOUNT_ID>.r2.cloudflarestorage.com",
    url = "s3://my-r2-bucket/"
)
}
}
\keyword{GDS}
\keyword{cloud}
