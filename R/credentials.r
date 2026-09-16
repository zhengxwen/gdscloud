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
# Internal: resolve a credential value
#
# A token (access_token, bearer_token) may be given as a string or as a
# function returning a string (a "token provider"). The function is
# called every time a URL is opened, so short-lived tokens (GCS OAuth2,
# Azure Entra ID) can be refreshed transparently; returning NULL or ""
# means "not available", and the next fallback is used.
#
.resolve_cred <- function(v)
{
    if (is.function(v))
    {
        v <- v()
        if (is.null(v)) return(NULL)
    }
    if (is.null(v)) return(NULL)
    if (!is.character(v) || length(v) != 1L)
    {
        stop("A credential must be a single character string, or a ",
            "function returning one.", call.=FALSE)
    }
    if (is.na(v)) NULL else v
}


#############################################################
# Internal: first non-empty string wins (URL > global > env)
#
.first_nonempty <- function(...)
{
    vals <- list(...)
    for (v in vals)
    {
        v <- .resolve_cred(v)
        if (!is.null(v) && nzchar(v))
            return(v)
    }
    ""
}


#############################################################
# Internal: validate the arguments of the config functions.
# Tokens (access_token, bearer_token) may be a string or a provider
# function; keys and other settings must be strings.
#
.check_token_arg <- function(v, name)
{
    if (is.null(v) || is.function(v)) return(invisible())
    if (!is.character(v) || length(v) != 1L || is.na(v))
    {
        stop("'", name, "' must be NULL, a single character string, or a ",
            "function returning a character string.", call.=FALSE)
    }
    invisible()
}

.check_string_arg <- function(v, name)
{
    if (is.null(v)) return(invisible())
    if (!is.character(v) || length(v) != 1L || is.na(v))
        stop("'", name, "' must be NULL or a single character string.",
            call.=FALSE)
    invisible()
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
    .check_token_arg(bearer_token, "bearer_token")
    if (is.null(url))
    {
        if (!is.null(bearer_token))
            .gdscloud_env$http_bearer_token <- bearer_token
    } else {
        scheme <- sub("://.*", "", url)
        if (!(scheme %in% c("http", "https")))
        {
            stop("'url' must start with 'http://' or 'https://'.",
                call.=FALSE)
        }
        .set_url_credentials(url, scheme, list(
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
    credentials=NULL, endpoint=NULL, path_style=NULL, url=NULL)
{
    .check_string_arg(aws_access_key_id, "aws_access_key_id")
    .check_string_arg(aws_secret_access_key, "aws_secret_access_key")
    .check_string_arg(session_token, "session_token")
    .check_string_arg(region, "region")
    if (!is.null(credentials) && !is.function(credentials))
    {
        stop("'credentials' must be NULL or a function returning a list ",
            "with 'aws_access_key_id', 'aws_secret_access_key' and ",
            "optionally 'session_token'.", call.=FALSE)
    }
    if (!is.null(endpoint))
    {
        if (!is.character(endpoint) || length(endpoint) != 1L ||
            is.na(endpoint))
        {
            stop("'endpoint' must be a single character string, e.g. ",
                "\"https://minio.example.org:9000\".", call.=FALSE)
        }
    }
    if (!is.null(path_style))
    {
        if (!is.logical(path_style) || length(path_style) != 1L)
            stop("'path_style' must be TRUE, FALSE or NA.", call.=FALSE)
    }
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
        if (!is.null(credentials))
            .gdscloud_env$aws_credentials <- credentials
        if (!is.null(endpoint))
            .gdscloud_env$aws_endpoint <- endpoint
        if (!is.null(path_style))
            .gdscloud_env$aws_path_style <- path_style
    } else {
        .set_url_credentials(url, "s3", list(
            aws_access_key_id     = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            aws_region            = region,
            aws_session_token     = session_token,
            aws_credentials       = credentials,
            aws_endpoint          = endpoint,
            aws_path_style        = path_style
        ))
    }
    invisible()
}


#############################################################
# Configure GCS credentials
#
gdsCloudConfigGCS <- function(access_token=NULL, url=NULL)
{
    .check_token_arg(access_token, "access_token")
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
    sas_token=NULL, access_token=NULL, endpoint_suffix=NULL, endpoint=NULL,
    url=NULL)
{
    .check_string_arg(account_name, "account_name")
    .check_string_arg(account_key, "account_key")
    .check_string_arg(sas_token, "sas_token")
    .check_token_arg(access_token, "access_token")
    .check_string_arg(endpoint_suffix, "endpoint_suffix")
    .check_string_arg(endpoint, "endpoint")
    fields <- list(
        azure_account_name    = account_name,
        azure_account_key     = account_key,
        azure_sas_token       = sas_token,
        azure_access_token    = access_token,
        azure_endpoint_suffix = endpoint_suffix,
        azure_endpoint        = endpoint
    )
    if (is.null(url))
    {
        for (nm in names(fields))
        {
            if (!is.null(fields[[nm]]))
                assign(nm, fields[[nm]], envir=.gdscloud_env)
        }
    } else {
        .set_url_credentials(url, "az", fields)
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

    # A credential provider function (URL-specific entry first, then the
    # global one) supplies access key, secret and session token as one
    # consistent set; it is consulted before the static values of its
    # layer and, when it yields a key, replaces the static chain entirely,
    # since mixing e.g. a fresh key with a stale token would be wrong.
    prov <- .call_aws_provider(m$aws_credentials, "URL-specific")
    if (is.null(prov))
        prov <- .call_aws_provider(.gdscloud_env$aws_credentials, "global")
    if (!is.null(prov))
    {
        access_key <- prov$aws_access_key_id
        secret_key <- prov$aws_secret_access_key
        session_token <- prov$session_token
    } else {
        access_key <- .first_nonempty(
            m$aws_access_key_id,
            .gdscloud_env$aws_access_key_id,
            Sys.getenv("AWS_ACCESS_KEY_ID", ""))
        secret_key <- .first_nonempty(
            m$aws_secret_access_key,
            .gdscloud_env$aws_secret_access_key,
            Sys.getenv("AWS_SECRET_ACCESS_KEY", ""))
        session_token <- .first_nonempty(
            m$aws_session_token,
            .gdscloud_env$aws_session_token,
            Sys.getenv("AWS_SESSION_TOKEN", ""))
    }

    list(
        access_key = access_key,
        secret_key = secret_key,
        region = .first_nonempty(
            prov$region,
            m$aws_region,
            .gdscloud_env$aws_region,
            Sys.getenv("AWS_DEFAULT_REGION", ""),
            "us-east-1"),
        session_token = session_token,
        # S3-compatible services: custom endpoint and addressing style
        endpoint = .first_nonempty(
            m$aws_endpoint,
            .gdscloud_env$aws_endpoint,
            Sys.getenv("AWS_ENDPOINT_URL_S3", ""),
            Sys.getenv("AWS_ENDPOINT_URL", "")),
        path_style = .first_nonna(
            m$aws_path_style,
            .gdscloud_env$aws_path_style,
            .env_logical("GDSCLOUD_S3_PATH_STYLE"))
    )
}


#############################################################
# Internal: call an AWS credential provider function
#
# Returns NULL when there is no provider or it returns NULL / no access
# key (meaning "not available"); otherwise a list with the character
# fields aws_access_key_id, aws_secret_access_key, session_token ("" if
# absent) and region ("" if absent). `aws_session_token` is accepted as
# an alias of `session_token` (the name used by aws.signature and paws).
#
.call_aws_provider <- function(f, what)
{
    if (is.null(f)) return(NULL)
    v <- f()
    if (is.null(v)) return(NULL)
    if (!is.list(v) || is.null(names(v)))
    {
        stop("The ", what, " AWS 'credentials' function must return a ",
            "named list (aws_access_key_id, aws_secret_access_key, ",
            "session_token) or NULL.", call.=FALSE)
    }
    get <- function(nm, alt=NULL)
    {
        x <- v[[nm]]
        if (is.null(x) && !is.null(alt)) x <- v[[alt]]
        if (is.null(x)) return("")
        if (!is.character(x) || length(x) != 1L)
        {
            stop("The ", what, " AWS 'credentials' function returned a ",
                "non-string value for '", nm, "'.", call.=FALSE)
        }
        if (is.na(x)) "" else x
    }
    key <- get("aws_access_key_id")
    if (!nzchar(key)) return(NULL)
    secret <- get("aws_secret_access_key")
    if (!nzchar(secret))
    {
        stop("The ", what, " AWS 'credentials' function returned an access ",
            "key without 'aws_secret_access_key'.", call.=FALSE)
    }
    list(aws_access_key_id = key, aws_secret_access_key = secret,
        session_token = get("session_token", "aws_session_token"),
        region = get("region"))
}


#############################################################
# Internal: first non-NA logical wins; NA when none is set
#
.first_nonna <- function(...)
{
    for (v in list(...))
    {
        if (!is.null(v) && length(v) == 1L && !is.na(v))
            return(as.logical(v))
    }
    NA
}

#############################################################
# Internal: a logical from an environment variable ("true"/"false",
# "1"/"0", "yes"/"no"); NA when unset or unrecognized
#
.env_logical <- function(name)
{
    v <- tolower(Sys.getenv(name, ""))
    if (v %in% c("true", "t", "1", "yes")) TRUE
    else if (v %in% c("false", "f", "0", "no")) FALSE
    else NA
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
    cs <- .azure_connection_string()
    list(
        account_name = .first_nonempty(
            m$azure_account_name,
            .gdscloud_env$azure_account_name,
            Sys.getenv("AZURE_STORAGE_ACCOUNT", ""),
            cs$account_name),
        account_key = .first_nonempty(
            m$azure_account_key,
            .gdscloud_env$azure_account_key,
            Sys.getenv("AZURE_STORAGE_KEY", ""),
            cs$account_key),
        sas_token = .first_nonempty(
            m$azure_sas_token,
            .gdscloud_env$azure_sas_token,
            Sys.getenv("AZURE_STORAGE_SAS_TOKEN", ""),
            cs$sas_token),
        access_token = .first_nonempty(
            m$azure_access_token,
            .gdscloud_env$azure_access_token,
            Sys.getenv("AZURE_STORAGE_ACCESS_TOKEN", "")),
        endpoint_suffix = .first_nonempty(
            m$azure_endpoint_suffix,
            .gdscloud_env$azure_endpoint_suffix,
            Sys.getenv("AZURE_STORAGE_ENDPOINT_SUFFIX", ""),
            cs$endpoint_suffix),
        endpoint = .first_nonempty(
            m$azure_endpoint,
            .gdscloud_env$azure_endpoint,
            Sys.getenv("AZURE_STORAGE_SERVICE_ENDPOINT", ""),
            cs$endpoint)
    )
}


#############################################################
# Internal: parse an Azure storage connection string, e.g.
#   "DefaultEndpointsProtocol=https;AccountName=..;AccountKey=..;
#    EndpointSuffix=core.windows.net" or "UseDevelopmentStorage=true"
# Returns a list with any of account_name, account_key, sas_token,
# endpoint_suffix, endpoint (all "" when unset).
#
.azure_connection_string <- function(
    s=Sys.getenv("AZURE_STORAGE_CONNECTION_STRING", ""))
{
    empty <- list(account_name="", account_key="", sas_token="",
        endpoint_suffix="", endpoint="")
    if (!is.character(s) || length(s) != 1L || is.na(s) || !nzchar(s))
        return(empty)
    parts <- strsplit(s, ";", fixed=TRUE)[[1L]]
    parts <- parts[grepl("=", parts, fixed=TRUE)]
    v <- sub("^[^=]*=", "", parts)
    names(v) <- sub("=.*$", "", parts)
    get <- function(k) if (k %in% names(v)) unname(v[[k]]) else ""
    ans <- list(
        account_name    = get("AccountName"),
        account_key     = get("AccountKey"),
        sas_token       = get("SharedAccessSignature"),
        endpoint_suffix = "",
        endpoint        = get("BlobEndpoint")
    )
    # "EndpointSuffix=core.windows.net" refers to all services; the blob
    # service lives under "blob." of it
    suffix <- get("EndpointSuffix")
    if (nzchar(suffix))
        ans$endpoint_suffix <- paste0("blob.", suffix)
    # the local emulator (Azurite) shorthand
    if (tolower(get("UseDevelopmentStorage")) == "true")
    {
        if (!nzchar(ans$account_name)) ans$account_name <- "devstoreaccount1"
        if (!nzchar(ans$account_key))
            ans$account_key <- paste0("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6",
                "IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
        if (!nzchar(ans$endpoint))
            ans$endpoint <- "http://127.0.0.1:10000/devstoreaccount1"
    }
    ans
}


#############################################################
# Internal: collect all configured credentials into a named list
#
.collect_credentials <- function()
{
    nms <- c(
        # S3
        "aws_access_key_id", "aws_secret_access_key", "aws_region",
        "aws_session_token", "aws_credentials", "aws_endpoint",
        "aws_path_style",
        # GCS
        "gcs_access_token",
        # Azure
        "azure_account_name", "azure_account_key", "azure_sas_token",
        "azure_access_token", "azure_endpoint_suffix", "azure_endpoint",
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
    # package options (timeouts, cache size), so workers behave alike
    ans$options <- .gdscloud_env$options
    ans$cache_size_mb <- .gdscloud_env$cache_size_mb
    ans
}


#############################################################
# Internal: install credentials on a worker (executed in child process)
#
.install_credentials <- function(creds)
{
    # ensure the package is loaded on the worker; this function carries the
    # gdscloud namespace as its enclosing environment, so .gdscloud_env then
    # resolves to the worker's own copy
    if (!requireNamespace("gdscloud", quietly=TRUE))
        return(invisible(FALSE))
    for (nm in names(creds))
        assign(nm, creds[[nm]], envir=.gdscloud_env)
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
            function(i, creds) .install_credentials(creds),
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
