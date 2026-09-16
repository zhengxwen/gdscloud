test_that("gdsCloudConfigS3 stores credentials", {
    gdsCloudConfigS3(
        aws_access_key_id = "test_key",
        aws_secret_access_key = "test_secret",
        region = "us-west-2",
        session_token = "test_token"
    )
    # credentials are internal, verify no error
    expect_invisible(gdsCloudConfigS3())
})

test_that("gdsCloudConfigGCS stores credentials", {
    gdsCloudConfigGCS(access_token = "test_gcs_token")
    expect_invisible(gdsCloudConfigGCS())
})

test_that("gdsCloudConfigAzure stores credentials", {
    gdsCloudConfigAzure(
        account_name = "testaccount",
        account_key = "testkey",
        sas_token = "testsas"
    )
    expect_invisible(gdsCloudConfigAzure())
})

test_that("gdsCloudConfigS3 accepts NULL without error", {
    expect_invisible(gdsCloudConfigS3(
        aws_access_key_id = NULL,
        aws_secret_access_key = NULL,
        region = NULL,
        session_token = NULL
    ))
})

test_that("gdsCloudExportCredentials handles trivial 'cl' values", {
    # serial: no-op, returns FALSE invisibly
    expect_false(suppressWarnings(gdsCloudExportCredentials(NULL)))
    expect_false(suppressWarnings(gdsCloudExportCredentials(FALSE)))
    # forking on Unix: no-op but returns TRUE invisibly
    if (.Platform$OS.type != "windows") {
        expect_true(gdsCloudExportCredentials(TRUE))
        expect_true(gdsCloudExportCredentials(2L))
    }
    # invalid input
    expect_error(gdsCloudExportCredentials("bad"))
})

test_that("gdsCloudExportCredentials pushes credentials to a PSOCK cluster", {
    skip_on_cran()
    skip_if_not_installed("parallel")
    # set a distinctive value in the parent
    gdsCloudConfigS3(aws_access_key_id = "export_test_key",
        aws_secret_access_key = "export_test_secret",
        region = "eu-west-1")
    cl <- parallel::makeCluster(1L)
    on.exit(parallel::stopCluster(cl), add = TRUE)
    # workers must have gdscloud installed; otherwise skip silently
    ok <- tryCatch(
        parallel::clusterEvalQ(cl, requireNamespace("gdscloud",
            quietly = TRUE))[[1L]],
        error = function(e) FALSE)
    skip_if_not(isTRUE(ok), "gdscloud not installed on worker")
    expect_true(gdsCloudExportCredentials(cl))
    got <- parallel::clusterEvalQ(cl, {
        env <- get(".gdscloud_env", envir = asNamespace("gdscloud"),
            inherits = FALSE)
        c(env$aws_access_key_id, env$aws_secret_access_key, env$aws_region)
    })[[1L]]
    expect_equal(got,
        c("export_test_key", "export_test_secret", "eu-west-1"))
    # the package options travel with the credentials
    gdsCloudOptions(timeout = 123)
    on.exit(gdsCloudOptions(timeout = 60), add = TRUE)
    expect_true(gdsCloudExportCredentials(cl))
    got <- parallel::clusterEvalQ(cl, gdscloud::gdsCloudOptions()$timeout)[[1L]]
    expect_equal(got, 123)
})


# --------------------------------------------------------------------------
# URL-specific credentials
# --------------------------------------------------------------------------

# Helper: save/restore the URL-specific registry around a test
.with_clean_url_creds <- function(code) {
    env <- get(".gdscloud_env", envir = asNamespace("gdscloud"),
        inherits = FALSE)
    old <- env$url_credentials
    env$url_credentials <- list()
    on.exit(env$url_credentials <- old, add = TRUE)
    force(code)
}

test_that("gdsCloudConfigS3 stores URL-specific credentials without clobbering globals", {
    .with_clean_url_creds({
        # set a global key first
        gdsCloudConfigS3(
            aws_access_key_id = "GLOBAL_KEY",
            aws_secret_access_key = "GLOBAL_SECRET"
        )
        # register a URL-specific key
        gdsCloudConfigS3(
            aws_access_key_id = "BUCKET_A_KEY",
            aws_secret_access_key = "BUCKET_A_SECRET",
            url = "s3://bucket-a/"
        )
        env <- get(".gdscloud_env", envir = asNamespace("gdscloud"),
            inherits = FALSE)
        # global unchanged
        expect_equal(env$aws_access_key_id, "GLOBAL_KEY")
        expect_equal(env$aws_secret_access_key, "GLOBAL_SECRET")
        # URL-specific entry stored under normalized key
        expect_true("s3://bucket-a/" %in% names(env$url_credentials))
        entry <- env$url_credentials[["s3://bucket-a/"]]
        expect_equal(entry$aws_access_key_id, "BUCKET_A_KEY")
        expect_equal(entry$scheme, "s3")
    })
})

test_that("URL without trailing slash is normalized", {
    .with_clean_url_creds({
        gdsCloudConfigS3(
            aws_access_key_id = "K",
            aws_secret_access_key = "S",
            url = "s3://bucket-a"
        )
        env <- get(".gdscloud_env", envir = asNamespace("gdscloud"),
            inherits = FALSE)
        expect_true("s3://bucket-a/" %in% names(env$url_credentials))
    })
})

test_that("all-NULL credential args with url removes the entry", {
    .with_clean_url_creds({
        gdsCloudConfigS3(
            aws_access_key_id = "K",
            aws_secret_access_key = "S",
            url = "s3://bucket-a/"
        )
        env <- get(".gdscloud_env", envir = asNamespace("gdscloud"),
            inherits = FALSE)
        expect_true("s3://bucket-a/" %in% names(env$url_credentials))
        gdsCloudConfigS3(url = "s3://bucket-a/")
        expect_false("s3://bucket-a/" %in% names(env$url_credentials))
        # removing a non-existent entry is a silent no-op
        expect_invisible(gdsCloudConfigS3(url = "s3://never-set/"))
    })
})

test_that("scheme mismatch in url errors", {
    .with_clean_url_creds({
        expect_error(
            gdsCloudConfigHTTP(bearer_token = "t", url = "htp://x/"),
            "http://|https://"
        )
        expect_error(
            gdsCloudConfigS3(aws_access_key_id = "k", url = "gs://x/"),
            "scheme"
        )
        expect_error(
            gdsCloudConfigGCS(access_token = "t", url = "s3://x/"),
            "scheme"
        )
        expect_error(
            gdsCloudConfigAzure(account_name = "a", url = "gs://x/"),
            "scheme"
        )
    })
})

test_that(".get_s3_credentials picks URL-specific over global", {
    .with_clean_url_creds({
        # scrub env vars that would otherwise leak in
        old <- Sys.getenv(c("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION", "AWS_SESSION_TOKEN"), names = TRUE,
            unset = NA)
        Sys.unsetenv(names(old))
        on.exit({
            for (nm in names(old)) {
                if (!is.na(old[[nm]]))
                    do.call(Sys.setenv,
                        setNames(list(old[[nm]]), nm))
            }
        }, add = TRUE)

        gdsCloudConfigS3(
            aws_access_key_id = "GLOBAL_KEY",
            aws_secret_access_key = "GLOBAL_SECRET",
            region = "us-east-1"
        )
        gdsCloudConfigS3(
            aws_access_key_id = "A_KEY",
            aws_secret_access_key = "A_SECRET",
            url = "s3://bucket-a/"
        )

        a <- gdscloud:::.get_s3_credentials("s3://bucket-a/file.gds")
        expect_equal(a$access_key, "A_KEY")
        expect_equal(a$secret_key, "A_SECRET")
        # region falls back to global since URL entry did not set it
        expect_equal(a$region, "us-east-1")

        g <- gdscloud:::.get_s3_credentials("s3://other-bucket/file.gds")
        expect_equal(g$access_key, "GLOBAL_KEY")
        expect_equal(g$secret_key, "GLOBAL_SECRET")

        # no URL supplied -> global
        n <- gdscloud:::.get_s3_credentials()
        expect_equal(n$access_key, "GLOBAL_KEY")
    })
})

test_that("longest-prefix match wins", {
    .with_clean_url_creds({
        gdsCloudConfigS3(
            aws_access_key_id = "SHORT",
            aws_secret_access_key = "s",
            url = "s3://bucket/"
        )
        gdsCloudConfigS3(
            aws_access_key_id = "LONG",
            aws_secret_access_key = "l",
            url = "s3://bucket/sub/"
        )
        deep <- gdscloud:::.get_s3_credentials("s3://bucket/sub/deep.gds")
        expect_equal(deep$access_key, "LONG")
        shallow <- gdscloud:::.get_s3_credentials("s3://bucket/top.gds")
        expect_equal(shallow$access_key, "SHORT")
    })
})

test_that(".get_gcs_credentials and .get_azure_credentials honor url", {
    .with_clean_url_creds({
        gdsCloudConfigGCS(access_token = "GLOBAL_GCS")
        gdsCloudConfigGCS(access_token = "URL_GCS", url = "gs://g1/")
        expect_equal(
            gdscloud:::.get_gcs_credentials("gs://g1/x")$access_token,
            "URL_GCS")
        expect_equal(
            gdscloud:::.get_gcs_credentials("gs://other/x")$access_token,
            "GLOBAL_GCS")

        gdsCloudConfigAzure(account_name = "GLOB", account_key = "gk")
        gdsCloudConfigAzure(account_name = "URL", account_key = "uk",
            url = "az://container/")
        az <- gdscloud:::.get_azure_credentials("az://container/blob")
        expect_equal(az$account_name, "URL")
        expect_equal(az$account_key, "uk")
        az2 <- gdscloud:::.get_azure_credentials("az://other/blob")
        expect_equal(az2$account_name, "GLOB")
    })
})

test_that("gdsCloudExportCredentials forwards url_credentials to workers", {
    skip_on_cran()
    skip_if_not_installed("parallel")
    .with_clean_url_creds({
        gdsCloudConfigS3(
            aws_access_key_id = "A_KEY",
            aws_secret_access_key = "A_SECRET",
            url = "s3://bucket-a/"
        )
        cl <- parallel::makeCluster(1L)
        on.exit(parallel::stopCluster(cl), add = TRUE)
        ok <- tryCatch(
            parallel::clusterEvalQ(cl, requireNamespace("gdscloud",
                quietly = TRUE))[[1L]],
            error = function(e) FALSE)
        skip_if_not(isTRUE(ok), "gdscloud not installed on worker")
        expect_true(gdsCloudExportCredentials(cl))
        got <- parallel::clusterEvalQ(cl, {
            env <- get(".gdscloud_env", envir = asNamespace("gdscloud"),
                inherits = FALSE)
            env$url_credentials[["s3://bucket-a/"]]
        })[[1L]]
        expect_equal(got$aws_access_key_id, "A_KEY")
        expect_equal(got$aws_secret_access_key, "A_SECRET")
        expect_equal(got$scheme, "s3")
    })
})


# --------------------------------------------------------------------------
# Credential provider functions
# --------------------------------------------------------------------------

test_that("token arguments accept functions, called when resolving", {
    .with_clean_url_creds({
        withr::local_envvar(GCS_ACCESS_TOKEN = NA, GDSCLOUD_HTTP_TOKEN = NA,
            AWS_ACCESS_KEY_ID = NA, AWS_SECRET_ACCESS_KEY = NA,
            AWS_SESSION_TOKEN = NA)
        env <- get(".gdscloud_env", envir = asNamespace("gdscloud"))
        old <- mget(c("gcs_access_token", "http_bearer_token",
            "aws_access_key_id", "aws_secret_access_key", "aws_session_token"),
            envir = env, ifnotfound = list(NULL))
        on.exit(for (nm in names(old)) assign(nm, old[[nm]], envir = env),
            add = TRUE)

        calls <- 0L
        gdsCloudConfigGCS(access_token = function() {
            calls <<- calls + 1L
            paste0("tok", calls)
        })
        # not called at configuration time, once per resolution
        expect_equal(calls, 0L)
        expect_equal(gdscloud:::.get_gcs_credentials("gs://b/k")$access_token,
            "tok1")
        expect_equal(gdscloud:::.get_gcs_credentials("gs://b/k")$access_token,
            "tok2")
        expect_equal(calls, 2L)

        # NULL / "" from the function falls through to the next layer
        gdsCloudConfigGCS(access_token = "GLOBAL")
        gdsCloudConfigGCS(access_token = function() NULL, url = "gs://b/")
        expect_equal(gdscloud:::.get_gcs_credentials("gs://b/k")$access_token,
            "GLOBAL")
        gdsCloudConfigGCS(access_token = function() "", url = "gs://b/")
        expect_equal(gdscloud:::.get_gcs_credentials("gs://b/k")$access_token,
            "GLOBAL")
        gdsCloudConfigGCS(access_token = function() "URL", url = "gs://b/")
        expect_equal(gdscloud:::.get_gcs_credentials("gs://b/k")$access_token,
            "URL")

        # the other token arguments
        gdsCloudConfigHTTP(bearer_token = function() "bt")
        expect_equal(gdscloud:::.get_http_credentials("https://x/y")$bearer_token,
            "bt")
        old_az <- mget(c("azure_access_token"), envir = env,
            ifnotfound = list(NULL))
        gdsCloudConfigAzure(access_token = function() "az")
        expect_equal(gdscloud:::.get_azure_credentials("az://c/b")$access_token,
            "az")
        assign("azure_access_token", old_az[[1L]], envir = env)

        # a function must return a single string (drop the URL entry first,
        # otherwise it would win before the global function is called)
        gdsCloudConfigGCS(url = "gs://b/")
        gdsCloudConfigGCS(access_token = function() 42)
        expect_error(gdscloud:::.get_gcs_credentials("gs://b/k"),
            "single character string")
        gdsCloudConfigGCS(access_token = function() c("a", "b"))
        expect_error(gdscloud:::.get_gcs_credentials("gs://b/k"),
            "single character string")
        # errors inside the provider propagate
        gdsCloudConfigGCS(access_token = function() stop("login failed"))
        expect_error(gdscloud:::.get_gcs_credentials("gs://b/k"), "login failed")

        # argument validation at configuration time: tokens may be
        # functions, keys must be strings
        expect_error(gdsCloudConfigGCS(access_token = 1), "function")
        expect_error(gdsCloudConfigHTTP(bearer_token = list()), "function")
        expect_error(gdsCloudConfigAzure(access_token = NA_character_),
            "function")
        expect_error(gdsCloudConfigS3(aws_access_key_id = function() "AK"),
            "single character string")
        expect_error(gdsCloudConfigS3(aws_secret_access_key = c("a", "b")),
            "single character string")
        expect_error(gdsCloudConfigS3(session_token = function() "t"),
            "single character string")
        expect_error(gdsCloudConfigAzure(account_key = function() "k"),
            "single character string")
        expect_error(gdsCloudConfigAzure(sas_token = function() "s"),
            "single character string")
        expect_error(gdsCloudConfigAzure(account_name = NA_character_),
            "single character string")
        gdsCloudConfigGCS(access_token = "")
    })
})

test_that("token provider functions travel to workers with the credentials", {
    skip_on_cran()
    skip_if_not_installed("parallel")
    .with_clean_url_creds({
        env <- get(".gdscloud_env", envir = asNamespace("gdscloud"))
        old <- env$gcs_access_token
        on.exit(assign("gcs_access_token", old, envir = env), add = TRUE)
        secret <- "from-parent-closure"
        gdsCloudConfigGCS(access_token = function() secret)
        cl <- parallel::makeCluster(1L)
        on.exit(parallel::stopCluster(cl), add = TRUE)
        ok <- tryCatch(
            parallel::clusterEvalQ(cl, requireNamespace("gdscloud",
                quietly = TRUE))[[1L]],
            error = function(e) FALSE)
        skip_if_not(isTRUE(ok), "gdscloud not installed on worker")
        expect_true(gdsCloudExportCredentials(cl))
        got <- parallel::clusterEvalQ(cl,
            gdscloud:::.get_gcs_credentials("gs://b/k")$access_token)[[1L]]
        expect_equal(got, "from-parent-closure")
    })
})
