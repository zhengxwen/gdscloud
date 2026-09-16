\name{gdsCloudOptions}
\alias{gdsCloudOptions}
\title{Package Options for Cloud Access}
\description{
    Get or set the options that control cloud transfers: connection and
    low-speed timeouts, and the default block-cache size.
}
\usage{
gdsCloudOptions(connect_timeout=NULL, timeout=NULL, cache_size=NULL)
}
\arguments{
    \item{connect_timeout}{maximum time in seconds to establish a
        connection (default 30)}
    \item{timeout}{low-speed timeout in seconds: a transfer that moves
        less than one byte per second for this long is aborted with an
        error (default 60)}
    \item{cache_size}{default block-cache size in megabytes for newly
        opened cloud streams (default 64); equivalent to
        \code{\link{gdsCloudCacheSize}}}
}
\details{
    Called without arguments, the function returns the current settings.
    Arguments that are \code{NULL} are left unchanged. Timeouts are read
    each time a file is opened and then apply to every request of every
    open stream.

    The initial values can be set persistently through R options or
    environment variables, e.g. in \code{.Rprofile} or \code{.Renviron}:
    \tabular{lll}{
        option \tab environment variable \tab default \cr
        \code{gdscloud.connect_timeout} \tab \code{GDSCLOUD_CONNECT_TIMEOUT}
            \tab 30 \cr
        \code{gdscloud.timeout} \tab \code{GDSCLOUD_TIMEOUT} \tab 60 \cr
        \code{gdscloud.cache_size_mb} \tab \code{GDSCLOUD_CACHE_SIZE_MB}
            \tab 64
    }
    They are consulted once, when the package is loaded.

    \code{\link{gdsCloudExportCredentials}} copies the current options to
    the workers of a parallel cluster together with the credentials.
}
\value{
    A list with the current settings (\code{connect_timeout},
    \code{timeout}, \code{cache_size}); invisibly when any argument was
    supplied.
}
\seealso{
    \code{\link{gdsCloudOpen}}, \code{\link{gdsCloudCacheSize}},
    \code{\link{gdsCloudExportCredentials}}
}
\examples{
# show the current settings
gdsCloudOptions()

# a slow or distant server: allow more time before giving up
gdsCloudOptions(connect_timeout=60, timeout=300)

# restore the defaults
gdsCloudOptions(connect_timeout=30, timeout=60, cache_size=64)
}
\keyword{GDS}
\keyword{cloud}
