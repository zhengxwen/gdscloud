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
