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
})
