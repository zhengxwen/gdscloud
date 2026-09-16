# Range reads against a local HTTP server: a correct server round-trips the
# data; servers that ignore or mis-report the Range must be rejected.

skip_if_not_installed("httpuv")
skip_if_not_installed("callr")

dir <- tempfile("gdscloud-range-")
dir.create(dir)
gds_path <- make_test_gds(file.path(dir, "test.gds"))
srv <- start_test_server(dir)
withr::defer(stop_test_server(srv), teardown_env())
withr::defer(unlink(dir, recursive = TRUE), teardown_env())

# reference values from the local file
loc <- openfn.gds(gds_path)
ref <- list(
    sample.id = read.gdsn(index.gdsn(loc, "sample.id")),
    position  = read.gdsn(index.gdsn(loc, "position")),
    geno      = read.gdsn(index.gdsn(loc, "geno")),
    text      = read.gdsn(index.gdsn(loc, "text"))
)
closefn.gds(loc)

test_that("a GDS file is read correctly over HTTP Range requests", {
    u <- paste0(srv$url, "/data/test.gds")
    gds <- gdsCloudOpen(u)
    on.exit(closefn.gds(gds), add = TRUE)
    expect_s3_class(gds, "gds.class")
    expect_identical(read.gdsn(index.gdsn(gds, "sample.id")), ref$sample.id)
    expect_identical(read.gdsn(index.gdsn(gds, "position")), ref$position)
    expect_identical(read.gdsn(index.gdsn(gds, "geno")), ref$geno)
    expect_identical(read.gdsn(index.gdsn(gds, "text")), ref$text)
    # random access into the big matrix crosses cache-block boundaries
    expect_identical(
        read.gdsn(index.gdsn(gds, "geno"), start = c(101, 1001),
            count = c(10, 20)),
        ref$geno[101:110, 1001:1020])
    lst <- gdsCloudList()
    expect_equal(lst$url, u)
    expect_equal(lst$file_size, file.size(gds_path))
})

test_that("a server that ignores Range requests is rejected, not misread", {
    # the size probe works (Content-Length), but the first read beyond
    # offset 0, which gdsfmt issues while opening the file, must fail
    # loudly instead of returning bytes from the start of the file
    expect_error(gdsCloudOpen(paste0(srv$url, "/norange/test.gds")),
        "does not support HTTP Range")
})

test_that("a wrong Content-Range start is detected", {
    expect_error(gdsCloudOpen(paste0(srv$url, "/badrange/test.gds")),
        "instead of the requested offset")
})

test_that("transient 503 responses are retried with back-off", {
    old <- gdsCloudOptions()
    on.exit(do.call(gdsCloudOptions, old), add = TRUE)

    gdsCloudOptions(max_retries = 3)
    before <- gdsCloudCacheInfo(verbose = FALSE)$retries
    gds <- gdsCloudOpen(paste0(srv$url, "/flaky/test.gds"))
    expect_identical(read.gdsn(index.gdsn(gds, "geno")), ref$geno)
    closefn.gds(gds)
    expect_gt(gdsCloudCacheInfo(verbose = FALSE)$retries, before)

    # without retries the flaky endpoint fails on two out of three requests
    gdsCloudOptions(max_retries = 0)
    before <- gdsCloudCacheInfo(verbose = FALSE)$retries
    errs <- vapply(1:3, function(i) tryCatch({
        g <- gdsCloudOpen(paste0(srv$url, "/flaky/test.gds"))
        closefn.gds(g)
        "ok"
    }, error = function(e) conditionMessage(e)), character(1))
    expect_true(any(grepl("HTTP 503", errs)))
    expect_true(any(grepl("SlowDown", errs)))
    expect_equal(gdsCloudCacheInfo(verbose = FALSE)$retries, before)
})

test_that("an S3-compatible endpoint round-trips through the S3 backend", {
    # The local server is not an S3 implementation, but with a custom
    # endpoint and path-style addressing "s3://data/test.gds" maps to
    # <endpoint>/data/test.gds, so the signed request must succeed and the
    # data round-trip through the S3 code path (as for MinIO, Ceph, R2...).
    env <- get(".gdscloud_env", envir = asNamespace("gdscloud"))
    nms <- c("aws_access_key_id", "aws_secret_access_key", "aws_endpoint",
        "aws_path_style")
    old <- mget(nms, envir = env, ifnotfound = list(NULL))
    on.exit(for (nm in nms) assign(nm, old[[nm]], envir = env), add = TRUE)

    gdsCloudConfigS3(aws_access_key_id = "AKIAEXAMPLE",
        aws_secret_access_key = "secret", endpoint = srv$url)
    gds <- gdsCloudOpen("s3://data/test.gds")
    expect_identical(read.gdsn(index.gdsn(gds, "geno")), ref$geno)
    expect_equal(gdsCloudList()$url, "s3://data/test.gds")
    closefn.gds(gds)

    # anonymous access to a public endpoint
    gdsCloudConfigS3(aws_access_key_id = "", aws_secret_access_key = "",
        endpoint = srv$url)
    gds <- gdsCloudOpen("s3://data/test.gds")
    expect_identical(read.gdsn(index.gdsn(gds, "text")), ref$text)
    closefn.gds(gds)

    # the error path keeps the S3 prefix and the server's body
    expect_error(gdsCloudOpen("s3://data/missing.gds"), "S3: HTTP 404")
})

test_that("an Azure custom endpoint round-trips through the Azure backend", {
    env <- get(".gdscloud_env", envir = asNamespace("gdscloud"))
    nms <- c("azure_account_name", "azure_account_key", "azure_sas_token",
        "azure_access_token", "azure_endpoint_suffix", "azure_endpoint")
    old <- mget(nms, envir = env, ifnotfound = list(NULL))
    on.exit(for (nm in nms) assign(nm, old[[nm]], envir = env), add = TRUE)
    withr::local_envvar(AZURE_STORAGE_CONNECTION_STRING = NA)

    # Shared Key (the server does not verify signatures, but the request
    # must be well-formed) and bearer token, both against <endpoint>/data/
    key <- paste0("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6",
        "tq/K1SZFPTOtr/KBHBeksoGMGw==")
    gdsCloudConfigAzure(account_name = "devstoreaccount1", account_key = key,
        sas_token = "", access_token = "", endpoint = srv$url)
    gds <- gdsCloudOpen("az://data/test.gds")
    expect_identical(read.gdsn(index.gdsn(gds, "position")), ref$position)
    closefn.gds(gds)

    gdsCloudConfigAzure(account_key = "", access_token = "tok")
    gds <- gdsCloudOpen("az://data/test.gds")
    expect_identical(read.gdsn(index.gdsn(gds, "text")), ref$text)
    closefn.gds(gds)

    expect_error(gdsCloudOpen("az://data/missing.gds"), "Azure: HTTP 404")
})

test_that("a bearer-token provider function is used when opening", {
    env <- get(".gdscloud_env", envir = asNamespace("gdscloud"))
    old <- list(tok = env$http_bearer_token, url = env$url_credentials)
    on.exit({
        env$http_bearer_token <- old$tok
        env$url_credentials <- old$url
    }, add = TRUE)
    withr::local_envvar(GDSCLOUD_HTTP_TOKEN = NA)
    env$url_credentials <- list()
    u <- paste0(srv$url, "/auth/test.gds")

    gdsCloudConfigHTTP(bearer_token = "")
    expect_error(gdsCloudOpen(u), "HTTP 401")

    calls <- 0L
    gdsCloudConfigHTTP(bearer_token = function() {
        calls <<- calls + 1L
        "secret-token"
    })
    gds <- gdsCloudOpen(u)
    expect_identical(read.gdsn(index.gdsn(gds, "sample.id")), ref$sample.id)
    closefn.gds(gds)
    gds <- gdsCloudOpen(u)
    closefn.gds(gds)
    # called once per open, not per request
    expect_equal(calls, 2L)

    # a provider returning a stale token fails with the server's message
    gdsCloudConfigHTTP(bearer_token = function() "expired")
    expect_error(gdsCloudOpen(u), "unauthorized")
})

test_that("an AWS credentials function is called once per open", {
    env <- get(".gdscloud_env", envir = asNamespace("gdscloud"))
    nms <- c("aws_access_key_id", "aws_secret_access_key", "aws_credentials",
        "aws_endpoint", "aws_path_style")
    old <- mget(nms, envir = env, ifnotfound = list(NULL))
    on.exit(for (nm in nms) assign(nm, old[[nm]], envir = env), add = TRUE)
    calls <- 0L
    gdsCloudConfigS3(endpoint = srv$url, credentials = function() {
        calls <<- calls + 1L
        list(aws_access_key_id = "AKIAEXAMPLE", aws_secret_access_key = "s",
            session_token = "tok")
    })
    gds <- gdsCloudOpen("s3://data/test.gds")
    expect_identical(read.gdsn(index.gdsn(gds, "sample.id")), ref$sample.id)
    read.gdsn(index.gdsn(gds, "geno"))
    closefn.gds(gds)
    expect_equal(calls, 1L)
})
