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

test_that("gdsCloudOptions gets and sets the options", {
    old <- gdsCloudOptions()
    on.exit(do.call(gdsCloudOptions, old), add = TRUE)
    expect_named(old, c("connect_timeout", "timeout", "cache_size"))
    expect_equal(old$connect_timeout, 30)
    expect_equal(old$timeout, 60)

    expect_invisible(gdsCloudOptions(connect_timeout = 5, timeout = 7))
    cur <- gdsCloudOptions()
    expect_equal(cur$connect_timeout, 5)
    expect_equal(cur$timeout, 7)
    expect_equal(cur$cache_size, old$cache_size)   # unchanged

    # cache_size and gdsCloudCacheSize() are the same setting
    gdsCloudOptions(cache_size = 128)
    env <- get(".gdscloud_env", envir = asNamespace("gdscloud"))
    expect_equal(env$cache_size_mb, 128)
    gdsCloudCacheSize(32)
    expect_equal(gdsCloudOptions()$cache_size, 32)

    expect_error(gdsCloudOptions(timeout = -1), "positive")
    expect_error(gdsCloudOptions(connect_timeout = "a"), "positive")
    expect_error(gdsCloudOptions(cache_size = c(1, 2)), "positive")
})
