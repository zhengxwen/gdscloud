# ===========================================================================
#
# credentials.r: Cloud credential configuration
#
# Copyright (C) 2026    Xiuwen Zheng
#
# This is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License Version 3 as
# published by the Free Software Foundation.
#
# gdscloud is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public
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
# Internal: first non-empty string wins (URL > global > env)
#
.first_nonempty <- function(...)
{
    vals <- list(...)
    for (v in vals)
    {
        if (!is.null(v) && nzchar(v))
            return(v)
    }
    ""
}


#############################################################
# Internal: normalize a URL prefix and validate its scheme
#
.normalize_url_prefix <- function(url, expected_scheme)
{
    if (!is.character(url) || length(url) != 1L || is.na(url) ||
        !nzchar(url))
    {
        stop("'url' must be a single non-empty character string.",
            call.=FALSE)
    }
    scheme <- sub("://.*", "", url)
    if (!identical(scheme, expected_scheme))
    {
        stop("URL scheme '", scheme, "://' does not match expected '",
            expected_scheme, "://'.", call.=FALSE)
    }
    # strip a trailing wildcard, then ensure a trailing '/'
    url <- sub("\\*+$", "", url)
    if (!endsWith(url, "/"))
        url <- paste0(url, "/")
    url
}


#############################################################
# Internal: store / remove a URL-specific credential entry
#
.set_url_credentials <- function(url, scheme, fields)
{
    key <- .normalize_url_prefix(url, scheme)
    # drop NULL fields
    fields <- fields[!vapply(fields, is.null, logical(1L))]
    tbl <- .gdscloud_env$url_credentials %||% list()
    if (length(fields) == 0L)
    {
        # remove the entry (no-op if it does not exist)
        tbl[[key]] <- NULL
    } else {
        fields$scheme <- scheme
        tbl[[key]] <- fields
    }
    .gdscloud_env$url_credentials <- tbl
    invisible()
}


#############################################################
# Internal: find the URL-specific entry with the longest matching
# prefix for `url` within the given scheme. Returns NULL when none.
#
.match_url_credentials <- function(url, scheme)
{
    if (!is.character(url) || length(url) != 1L || is.na(url) ||
        !nzchar(url))
    {
        return(NULL)
    }
    tbl <- .gdscloud_env$url_credentials
    if (!length(tbl))
        return(NULL)
    keys <- names(tbl)
    # restrict to entries of the requested scheme
    schemes <- vapply(tbl, function(e) e$scheme %||% "", character(1L))
    sel <- schemes == scheme & vapply(keys, startsWith, logical(1L),
        x=url)
    if (!any(sel))
        return(NULL)
    idx <- which(sel)
    # longest-prefix wins
    best <- idx[which.max(nchar(keys[idx]))]
    tbl[[best]]
}


#############################################################
# Configure HTTP/HTTPS credentials (optional Bearer token)
#
gdsCloudConfigHTTP <- function(bearer_token=NULL, url=NULL)
{
    if (is.null(url))
    {
        if (!is.null(bearer_token))
            .gdscloud_env$http_bearer_token <- bearer_token
    } else {
        .set_url_credentials(url, sub("://.*", "", url), list(
            http_bearer_token = bearer_token
        ))
    }
    invisible()
}


#############################################################
# Configure AWS S3 credentials
#
gdsCloudConfigS3 <- function(aws_access_key_id=NULL,
    aws_secret_access_key=NULL, region=NULL, session_token=NULL,
    url=NULL)
{
    if (is.null(url))
    {
        if (!is.null(aws_access_key_id))
            .gdscloud_env$aws_access_key_id <- aws_access_key_id
        if (!is.null(aws_secret_access_key))
            .gdscloud_env$aws_secret_access_key <- aws_secret_access_key
        if (!is.null(region))
            .gdscloud_env$aws_region <- region
        if (!is.null(session_token))
            .gdscloud_env$aws_session_token <- session_token
    } else {
        .set_url_credentials(url, "s3", list(
            aws_access_key_id     = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            aws_region            = region,
            aws_session_token     = session_token
        ))
    }
    invisible()
}


#############################################################
# Configure GCS credentials
#
gdsCloudConfigGCS <- function(access_token=NULL, url=NULL)
{
    if (is.null(url))
    {
        if (!is.null(access_token))
            .gdscloud_env$gcs_access_token <- access_token
    } else {
        .set_url_credentials(url, "gs", list(
            gcs_access_token = access_token
        ))
    }
    invisible()
}


#############################################################
# Configure Azure credentials
#
gdsCloudConfigAzure <- function(account_name=NULL, account_key=NULL,
    sas_token=NULL, url=NULL)
{
    if (is.null(url))
    {
        if (!is.null(account_name))
            .gdscloud_env$azure_account_name <- account_name
        if (!is.null(account_key))
            .gdscloud_env$azure_account_key <- account_key
        if (!is.null(sas_token))
            .gdscloud_env$azure_sas_token <- sas_token
    } else {
        .set_url_credentials(url, "az", list(
            azure_account_name = account_name,
            azure_account_key  = account_key,
            azure_sas_token    = sas_token
        ))
    }
    invisible()
}


#############################################################
# Internal: get HTTP/HTTPS credentials
#
.get_http_credentials <- function(url=NULL)
{
    scheme <- sub("://.*", "", url)
    m <- .match_url_credentials(url, scheme)
    list(
        bearer_token = .first_nonempty(
            m$http_bearer_token,
            .gdscloud_env$http_bearer_token,
            Sys.getenv("GDSCLOUD_HTTP_TOKEN", ""))
    )
}


#############################################################
# Internal: get S3 credentials
# Priority: URL-specific entry > global (.gdscloud_env) > env vars
#
.get_s3_credentials <- function(url=NULL)
{
    m <- .match_url_credentials(url, "s3")
    list(
        access_key = .first_nonempty(
            m$aws_access_key_id,
            .gdscloud_env$aws_access_key_id,
            Sys.getenv("AWS_ACCESS_KEY_ID", "")),
        secret_key = .first_nonempty(
            m$aws_secret_access_key,
            .gdscloud_env$aws_secret_access_key,
            Sys.getenv("AWS_SECRET_ACCESS_KEY", "")),
        region = .first_nonempty(
            m$aws_region,
            .gdscloud_env$aws_region,
            Sys.getenv("AWS_DEFAULT_REGION", ""),
            "us-east-1"),
        session_token = .first_nonempty(
            m$aws_session_token,
            .gdscloud_env$aws_session_token,
            Sys.getenv("AWS_SESSION_TOKEN", ""))
    )
}


#############################################################
# Internal: get GCS credentials
#
.get_gcs_credentials <- function(url=NULL)
{
    m <- .match_url_credentials(url, "gs")
    list(
        access_token = .first_nonempty(
            m$gcs_access_token,
            .gdscloud_env$gcs_access_token,
            Sys.getenv("GCS_ACCESS_TOKEN", ""))
    )
}


#############################################################
# Internal: get Azure credentials
#
.get_azure_credentials <- function(url=NULL)
{
    m <- .match_url_credentials(url, "az")
    list(
        account_name = .first_nonempty(
            m$azure_account_name,
            .gdscloud_env$azure_account_name,
            Sys.getenv("AZURE_STORAGE_ACCOUNT", "")),
        account_key = .first_nonempty(
            m$azure_account_key,
            .gdscloud_env$azure_account_key,
            Sys.getenv("AZURE_STORAGE_KEY", "")),
        sas_token = .first_nonempty(
            m$azure_sas_token,
            .gdscloud_env$azure_sas_token,
            Sys.getenv("AZURE_STORAGE_SAS_TOKEN", ""))
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
        "azure_account_name", "azure_account_key", "azure_sas_token",
        # HTTP
        "http_bearer_token"
    )
    ans <- lapply(nms, function(nm) .gdscloud_env[[nm]])
    names(ans) <- nms
    # drop unset entries
    ans <- ans[!vapply(ans, is.null, logical(1L))]
    # include URL-specific registry if non-empty
    url_tbl <- .gdscloud_env$url_credentials
    if (length(url_tbl))
        ans$url_credentials <- url_tbl
    ans
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
