\name{gdsCloudExportCredentials}
\alias{gdsCloudExportCredentials}
\title{Export Cloud Credentials to Child Processes}
\description{
    Copy cloud storage credentials configured in the current R session to
    worker processes of a parallel cluster, typically the one used by
    \code{SeqArray::seqParallel()}. This is required for PSOCK / SOCK /
    MPI / BiocParallel clusters, which do not inherit the parent R
    session's state.
}
\usage{
gdsCloudExportCredentials(cl)
}
\arguments{
    \item{cl}{a cluster specification following the convention of the
        \code{cl} argument to \code{\link[SeqArray]{seqParallel}}:
        \code{NULL} or \code{FALSE} (serial processing, no-op);
        \code{TRUE} or a numeric value (forking on Unix);
        a \code{cluster} object from the \pkg{parallel} package
        (e.g. created by \code{\link[parallel]{makeCluster}}); or a
        \code{BiocParallelParam} object from the \pkg{BiocParallel}
        package.}
}
\details{
    Credentials set via \code{\link{gdsCloudConfigS3}},
    \code{\link{gdsCloudConfigGCS}} and \code{\link{gdsCloudConfigAzure}}
    are stored in an internal environment of the \pkg{gdscloud} package
    in the parent R session, and are \emph{not} automatically visible to
    workers spawned by socket-based clusters. Call
    \code{gdsCloudExportCredentials(cl)} once after creating the cluster
    and configuring credentials in the parent session, and before
    invoking \code{\link[SeqArray]{seqParallel}}.

    On Unix, when \code{cl} is \code{TRUE} or a numeric value,
    \code{seqParallel()} uses forking and child processes inherit the
    parent's in-memory credentials automatically, so this function is a
    no-op in that case. On Windows, forking is not supported and an
    explicit cluster object should be created and passed to this
    function.

    Environment variables (e.g. \code{AWS_ACCESS_KEY_ID}) on the worker
    machines are \emph{not} modified.
}
\value{
    Invisibly returns \code{TRUE} if credentials were exported (or
    inherited via forking), and \code{FALSE} otherwise.
}
\seealso{
    \code{\link{gdsCloudConfigS3}}, \code{\link{gdsCloudConfigGCS}},
    \code{\link{gdsCloudConfigAzure}}, \code{\link{gdsCloudOpen}}
}
\examples{
\dontrun{
library(parallel)
library(SeqArray)

# configure credentials in the parent session
gdsCloudConfigS3(aws_access_key_id="...", aws_secret_access_key="...")

# create a PSOCK cluster and export credentials to the workers
cl <- makeCluster(4)
gdsCloudExportCredentials(cl)

# run a parallel analysis on a cloud-hosted GDS file
seqParallel(cl, "s3://my-bucket/my-file.gds", FUN=function(f) {
    seqSummary(f)
})

stopCluster(cl)
}
}
\keyword{GDS}
\keyword{cloud}
\keyword{parallel}
