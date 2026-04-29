test_that("gdsCloudOpen rejects invalid URL schemes", {
    expect_error(gdsCloudOpen("http://example.com/file.gds"),
        "Unsupported URL scheme")
    expect_error(gdsCloudOpen("ftp://example.com/file.gds"),
        "Unsupported URL scheme")
})

test_that("gdsCloudOpen validates url argument", {
    expect_error(gdsCloudOpen(123))
    expect_error(gdsCloudOpen(c("s3://a/b", "s3://c/d")))
    expect_error(gdsCloudOpen(NULL))
})

test_that("gdsCloudOpen validates allow.error argument", {
    expect_error(gdsCloudOpen("s3://bucket/key", allow.error="yes"))
    expect_error(gdsCloudOpen("s3://bucket/key", allow.error=c(TRUE, FALSE)))
})
