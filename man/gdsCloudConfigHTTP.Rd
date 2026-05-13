\name{gdsCloudConfigHTTP}
\alias{gdsCloudConfigHTTP}
\title{Configure HTTP/HTTPS Credentials}
\description{
    Set an optional Bearer token for authenticated HTTP/HTTPS access.
    Credentials set via this function take priority over environment
    variables. URL-specific credentials can also be registered so that
    different URLs use different tokens.
}
\usage{
gdsCloudConfigHTTP(bearer_token=NULL, url=NULL)
}
\arguments{
    \item{bearer_token}{a Bearer token string for HTTP Authorization
        header (e.g. an OAuth2 access token or API token)}
    \item{url}{optional URL prefix (e.g.
        \code{"https://private.example.com/"}) to associate the given
        credentials with. When \code{NULL} (the default) the credentials
        are stored as the global defaults. Passing \code{url} with
        \code{bearer_token=NULL} removes the entry for that prefix.}
}
\details{
    If not set explicitly, the Bearer token falls back to the
    \code{GDSCLOUD_HTTP_TOKEN} environment variable.

    When a URL is opened via \code{\link{gdsCloudOpen}}, credentials
    are resolved in the following order (the first non-empty value
    wins):
    \enumerate{
        \item the URL-specific entry whose registered prefix is the
            longest prefix of the opened URL;
        \item the global value set by this function with
            \code{url=NULL};
        \item the \code{GDSCLOUD_HTTP_TOKEN} environment variable.
    }

    If no token is configured, the request is sent without an
    Authorization header (suitable for public URLs).
}
\value{
    None (invisible).
}
\seealso{
    \code{\link{gdsCloudOpen}}, \code{\link{gdsCloudConfigS3}}
}
\examples{
\dontrun{
# Set a global Bearer token
gdsCloudConfigHTTP(bearer_token = "my-secret-token")

# Set a URL-specific token
gdsCloudConfigHTTP(
    bearer_token = "token-for-private-server",
    url = "https://private.example.com/"
)

# Open a public HTTPS file (no token needed)
gds <- gdsCloudOpen("https://example.com/data/file.gds")
closefn.gds(gds)
}
}
\keyword{GDS}
\keyword{cloud}
