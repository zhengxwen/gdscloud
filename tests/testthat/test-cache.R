test_that("gdsCloudCacheSize sets and returns cache size", {
    result <- gdsCloudCacheSize(128)
    expect_equal(result, 128)
    # reset to default
    gdsCloudCacheSize(64)
})

test_that("gdsCloudCacheSize validates input", {
    expect_error(gdsCloudCacheSize(-1))
    expect_error(gdsCloudCacheSize(0))
    expect_error(gdsCloudCacheSize("big"))
    expect_error(gdsCloudCacheSize(c(32, 64)))
})

test_that("gdsCloudCacheClear runs without error", {
    expect_invisible(gdsCloudCacheClear())
})

test_that("gdsCloudCacheInfo returns a list", {
    info <- gdsCloudCacheInfo(verbose=FALSE)
    expect_true(is.list(info) || is.null(info))
})
