# ===========================================================================
#
# credentials.r: Cloud credential configuration
#
# Copyright (C) 2026    Xiuwen Zheng
#
# This is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License Version 3 as
# published by the Free Software Foundation.
#
# gdscloud is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with gdscloud.
# If not, see <http://www.gnu.org/licenses/>.


#############################################################
# Internal: null-coalescing operator
#
`%||%` <- function(x, y)
{
    if (is.null(x)) y else x
}


#############################################################
# Configure AWS S3 credentials
#
gdsCloudConfigS3 <- function(aws_access_key_id=NULL,
    aws_secret_access_key=NULL, region=NULL, session_token=NULL)
{
    if (!is.null(aws_access_key_id))
        .gdscloud_env$aws_access_key_id <- aws_access_key_id
    if (!is.null(aws_secret_access_key))
        .gdscloud_env$aws_secret_access_key <- aws_secret_access_key
    if (!is.null(region))
        .gdscloud_env$aws_region <- region
    if (!is.null(session_token))
        .gdscloud_env$aws_session_token <- session_token
    invisible()
}


#############################################################
# Configure GCS credentials
#
gdsCloudConfigGCS <- function(access_token=NULL)
{
    if (!is.null(access_token))
        .gdscloud_env$gcs_access_token <- access_token
    invisible()
}


#############################################################
# Configure Azure credentials
#
gdsCloudConfigAzure <- function(account_name=NULL, account_key=NULL,
    sas_token=NULL)
{
    if (!is.null(account_name))
        .gdscloud_env$azure_account_name <- account_name
    if (!is.null(account_key))
        .gdscloud_env$azure_account_key <- account_key
    if (!is.null(sas_token))
        .gdscloud_env$azure_sas_token <- sas_token
    invisible()
}


#############################################################
# Internal: get S3 credentials (R config > env vars)
#
.get_s3_credentials <- function()
{
    list(
        access_key = .gdscloud_env$aws_access_key_id %||%
            Sys.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_key = .gdscloud_env$aws_secret_access_key %||%
            Sys.getenv("AWS_SECRET_ACCESS_KEY", ""),
        region = .gdscloud_env$aws_region %||%
            Sys.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        session_token = .gdscloud_env$aws_session_token %||%
            Sys.getenv("AWS_SESSION_TOKEN", "")
    )
}


#############################################################
# Internal: get GCS credentials
#
.get_gcs_credentials <- function()
{
    list(
        access_token = .gdscloud_env$gcs_access_token %||%
            Sys.getenv("GCS_ACCESS_TOKEN", "")
    )
}


#############################################################
# Internal: get Azure credentials
#
.get_azure_credentials <- function()
{
    list(
        account_name = .gdscloud_env$azure_account_name %||%
            Sys.getenv("AZURE_STORAGE_ACCOUNT", ""),
        account_key = .gdscloud_env$azure_account_key %||%
            Sys.getenv("AZURE_STORAGE_KEY", ""),
        sas_token = .gdscloud_env$azure_sas_token %||%
            Sys.getenv("AZURE_STORAGE_SAS_TOKEN", "")
    )
}


#############################################################
# Internal: collect all configured credentials into a named list
#
.collect_credentials <- function()
{
    nms <- c(
        # S3
        "aws_access_key_id", "aws_secret_access_key", "aws_region",
        "aws_session_token",
        # GCS
        "gcs_access_token",
        # Azure
        "azure_account_name", "azure_account_key", "azure_sas_token"
    )
    ans <- lapply(nms, function(nm) .gdscloud_env[[nm]])
    names(ans) <- nms
    # drop unset entries
    ans[!vapply(ans, is.null, logical(1L))]
}


#############################################################
# Internal: install credentials on a worker (executed in child process)
#
.install_credentials <- function(creds)
{
    # ensure the package is attached on the worker
    if (!requireNamespace("gdscloud", quietly=TRUE))
        return(invisible(FALSE))
    env <- get(".gdscloud_env", envir=asNamespace("gdscloud"),
        inherits=FALSE)
    for (nm in names(creds))
        assign(nm, creds[[nm]], envir=env)
    invisible(TRUE)
}


#############################################################
# Export cloud credentials to child processes
#
gdsCloudExportCredentials <- function(cl)
{
    # nothing to do for serial processing
    if (is.null(cl) || isFALSE(cl))
        return(invisible(FALSE))

    creds <- .collect_credentials()

    # PSOCK / SOCK / MPI cluster from the 'parallel' package
    if (inherits(cl, "cluster"))
    {
        if (!requireNamespace("parallel", quietly=TRUE))
            stop("The 'parallel' package is required.")
        parallel::clusterCall(cl, .install_credentials, creds)
        return(invisible(TRUE))
    }

    # BiocParallel cluster
    if (inherits(cl, "BiocParallelParam"))
    {
        if (!requireNamespace("BiocParallel", quietly=TRUE))
            stop("The 'BiocParallel' package is required.")
        n <- BiocParallel::bpnworkers(cl)
        BiocParallel::bplapply(seq_len(n),
            function(i, creds) gdscloud:::.install_credentials(creds),
            creds=creds, BPPARAM=cl)
        return(invisible(TRUE))
    }

    # TRUE or numeric: forking on Unix inherits the parent environment,
    # so there is nothing to distribute. On Windows, seqParallel() will
    # internally build a PSOCK cluster; in that case the user should
    # create the cluster explicitly and pass it to this function.
    if (isTRUE(cl) || is.numeric(cl))
    {
        if (.Platform$OS.type == "windows")
        {
            warning("On Windows, pass an explicit cluster object ",
                "(e.g. from parallel::makeCluster) to ",
                "gdsCloudExportCredentials(); credentials cannot be ",
                "exported via a numeric value.",
                call.=FALSE)
            return(invisible(FALSE))
        }
        # Unix forking: credentials are inherited by the child processes
        return(invisible(TRUE))
    }

    stop("Invalid 'cl': must be NULL, FALSE, TRUE, a numeric value, ",
        "a 'cluster' object, or a 'BiocParallelParam' object.")
}
