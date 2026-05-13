test_that("gdsCloudSchemes includes http and https", {
    schemes <- gdsCloudSchemes()
    expect_true("http" %in% names(schemes))
    expect_true("https" %in% names(schemes))
    expect_equal(unname(schemes["http"]), "HTTP")
    expect_equal(unname(schemes["https"]), "HTTPS")
})

test_that("gdsCloudConfigHTTP stores and retrieves bearer token", {
    # clear any existing token
    .gdscloud_env <- get(".gdscloud_env", envir=asNamespace("gdscloud"))
    old_token <- .gdscloud_env$http_bearer_token
    on.exit(.gdscloud_env$http_bearer_token <- old_token)

    gdsCloudConfigHTTP(bearer_token = "test-token-123")
    cred <- gdscloud:::.get_http_credentials("https://example.com/file.gds")
    expect_equal(cred$bearer_token, "test-token-123")
})

test_that("gdsCloudConfigHTTP supports URL-specific credentials", {
    .gdscloud_env <- get(".gdscloud_env", envir=asNamespace("gdscloud"))
    old_creds <- .gdscloud_env$url_credentials
    on.exit(.gdscloud_env$url_credentials <- old_creds)

    gdsCloudConfigHTTP(bearer_token = "specific-token",
        url = "https://private.example.com/")

    cred <- gdscloud:::.get_http_credentials(
        "https://private.example.com/data/file.gds")
    expect_equal(cred$bearer_token, "specific-token")
})

test_that(".open_http rejects non-http URLs at C level", {
    # ftp:// will fail the http:// / https:// validation in C
    expect_error(
        gdscloud:::.open_http("ftp://example.com/file.gds"),
        "must start with"
    )
})

test_that("gdsCloudOpen accepts http:// and https:// schemes", {
    # These should fail with a network/access error, not a scheme error
    err1 <- tryCatch(gdsCloudOpen("http://invalid.test/nonexistent.gds"),
        error = function(e) conditionMessage(e))
    expect_false(grepl("Unsupported URL scheme", err1))

    err2 <- tryCatch(gdsCloudOpen("https://invalid.test/nonexistent.gds"),
        error = function(e) conditionMessage(e))
    expect_false(grepl("Unsupported URL scheme", err2))
})
