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
    region=NULL, session_token=NULL, credentials=NULL, endpoint=NULL,
    path_style=NULL, url=NULL)

gdsCloudConfigGCS(access_token=NULL, url=NULL)

gdsCloudConfigAzure(account_name=NULL, account_key=NULL, sas_token=NULL,
    access_token=NULL, endpoint_suffix=NULL, endpoint=NULL, url=NULL)
}
\arguments{
    \item{aws_access_key_id}{AWS access key ID}
    \item{aws_secret_access_key}{AWS secret access key}
    \item{region}{AWS region (default: \code{"us-east-1"})}
    \item{session_token}{AWS session token for temporary credentials}
    \item{credentials}{a function with no arguments returning a named
        list with \code{aws_access_key_id}, \code{aws_secret_access_key}
        and, for temporary credentials, \code{session_token} (optionally
        also \code{region}), or \code{NULL} when no credentials are
        available; called each time a file is opened. See
        \sQuote{Token provider functions} below}
    \item{endpoint}{for \code{gdsCloudConfigS3}: base URL of an
        S3-compatible service, e.g. \code{"https://minio.example.org:9000"}
        or \code{"https://<account>.r2.cloudflarestorage.com"}; \code{""}
        or unset selects Amazon S3
        (\code{https://<bucket>.s3.<region>.amazonaws.com}). For
        \code{gdsCloudConfigAzure}: full base URL of the blob service,
        overriding the account host, e.g.
        \code{"http://127.0.0.1:10000/devstoreaccount1"} for the Azurite
        emulator; \code{az://container/blob} is then served from
        \code{<endpoint>/container/blob}. In both cases the scheme
        defaults to \code{https://} when omitted and a path prefix is
        allowed.}
    \item{path_style}{\code{TRUE} to address objects as
        \code{https://<host>/<bucket>/<key>} (path style), \code{FALSE}
        for \code{https://<bucket>.<host>/<key>} (virtual-hosted style),
        \code{NA} (the default) for path style with a custom
        \code{endpoint} and virtual-hosted style with Amazon S3}
    \item{access_token}{for \code{gdsCloudConfigGCS}: a Google Cloud
        OAuth2 access token. For \code{gdsCloudConfigAzure}: an Azure
        OAuth2 access token for the storage resource (Microsoft Entra ID,
        e.g. from
        \code{az account get-access-token --resource https://storage.azure.com/}
        or a managed identity); used when no SAS token is set. Either may
        be a function returning the token, see \sQuote{Token provider
        functions} below}
    \item{account_name}{Azure storage account name}
    \item{account_key}{Azure storage account key}
    \item{sas_token}{Azure SAS (Shared Access Signature) token}
    \item{endpoint_suffix}{DNS suffix of the blob service, default
        \code{"blob.core.windows.net"}; e.g.
        \code{"blob.core.chinacloudapi.cn"} (Azure China) or
        \code{"blob.core.usgovcloudapi.net"} (Azure Government)}
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
            \code{AZURE_STORAGE_SAS_TOKEN}, \code{AZURE_STORAGE_ACCESS_TOKEN},
            \code{AZURE_STORAGE_ENDPOINT_SUFFIX},
            \code{AZURE_STORAGE_SERVICE_ENDPOINT}; and, as a last resort,
            the fields of \code{AZURE_STORAGE_CONNECTION_STRING}
            (\code{AccountName}, \code{AccountKey},
            \code{SharedAccessSignature}, \code{EndpointSuffix},
            \code{BlobEndpoint}, \code{UseDevelopmentStorage=true})
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

    \subsection{Token provider functions}{
    The token arguments, \code{access_token} of \code{gdsCloudConfigGCS}
    and \code{gdsCloudConfigAzure} and \code{bearer_token} of
    \code{\link{gdsCloudConfigHTTP}}, may also be a function with no
    arguments that returns the token as a single character string. The
    function is called each time a file is opened, so tokens that expire
    (Google OAuth2 and Microsoft Entra ID access tokens last about an
    hour) are refreshed without reconfiguring. Returning \code{NULL} or
    \code{""} means no token is available and the next fallback (global
    setting, environment variable) is tried. A function is only called
    when the file is opened; requests made later on the same open file
    keep using the token obtained then.

    AWS temporary credentials (STS, IAM Identity Center / SSO, EC2 or
    EKS instance roles) are a set of three values that expire together,
    so \code{gdsCloudConfigS3} takes one \code{credentials} function
    returning the whole set as a named list instead of per-field
    functions. When it returns an access key, the set is used as is (no
    mixing with static values); when it returns \code{NULL} or no access
    key, the static settings and environment variables are used. Static
    AWS access keys, the Azure account key and SAS tokens are long-lived
    and are given as strings.

    This is the intended way to plug in the login machinery of dedicated
    packages and tools, e.g. \pkg{gargle} or \pkg{googleAuthR} for
    Google, \pkg{AzureAuth} for Azure, and the AWS CLI
    (\code{aws configure export-credentials}) or \pkg{aws.signature}
    for AWS; see the examples.
    }

    \subsection{Azure authentication}{
    When several Azure credentials are configured, a SAS token is used
    first, then an OAuth2 \code{access_token}, then the Shared Key
    \code{account_key}; with none of them the request is anonymous
    (public containers). Requests to a sovereign cloud only need
    \code{endpoint_suffix}; a self-hosted or emulated service
    (Azurite) needs the full \code{endpoint}.
    }

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
    \code{\link{gdsCloudOpen}}, \code{\link{gdsCloudConfigHTTP}},
    \code{\link{gdsCloudExportCredentials}}
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
# Token provider functions: called every time a file is opened
# Google Cloud Storage via gargle (user or service-account login)
gdsCloudConfigGCS(access_token = function()
    gargle::token_fetch(scopes = "https://www.googleapis.com/auth/devstorage.read_only")$credentials$access_token)

# ... or via the gcloud CLI
gdsCloudConfigGCS(access_token = function()
    system2("gcloud", c("auth", "print-access-token"), stdout = TRUE))

# Azure via AzureAuth (Entra ID)
gdsCloudConfigAzure(account_name = "mystorageaccount", access_token = function()
    AzureAuth::get_azure_token("https://storage.azure.com/",
        tenant = "mytenant", app = "myapp")$credentials$access_token)

# AWS temporary credentials from the AWS CLI's credential chain
# (profiles, SSO, assume-role, instance roles): the whole set at once
gdsCloudConfigS3(credentials = function() {
    out <- system2("aws", c("configure", "export-credentials",
        "--format", "process"), stdout = TRUE)
    j <- jsonlite::fromJSON(paste(out, collapse = ""))
    list(aws_access_key_id = j$AccessKeyId,
        aws_secret_access_key = j$SecretAccessKey,
        session_token = j$SessionToken)
})

# ... or via aws.signature (field names are accepted as returned)
gdsCloudConfigS3(credentials = function() {
    cr <- aws.signature::locate_credentials()
    list(aws_access_key_id = cr$key, aws_secret_access_key = cr$secret,
        session_token = cr$session_token, region = cr$region)
})

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

# Azure with an Entra ID token, in a sovereign cloud
gdsCloudConfigAzure(
    account_name = "mystorageaccount",
    access_token = "eyJ0eXAiOiJKV1QiLCJhbGci...",
    endpoint_suffix = "blob.core.chinacloudapi.cn"
)

# Azurite emulator (the well-known development account and key)
gdsCloudConfigAzure(
    account_name = "devstoreaccount1",
    account_key = paste0("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6",
        "IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="),
    endpoint = "http://127.0.0.1:10000/devstoreaccount1"
)

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
