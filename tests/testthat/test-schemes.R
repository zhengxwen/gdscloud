test_that("gdsCloudSchemes lists all supported schemes", {
    schemes <- gdsCloudSchemes()
    expect_type(schemes, "character")
    expect_setequal(names(schemes), c("http", "https", "s3", "gs", "az"))
    expect_false(any(is.na(schemes)))
    expect_true(all(nzchar(schemes)))
})

test_that("gdsCloudOpen error lists the supported schemes", {
    err <- tryCatch(gdsCloudOpen("ftp://example.com/file.gds"),
        error = function(e) conditionMessage(e))
    expect_match(err, "Unsupported URL scheme")
    # the message is derived from gdsCloudSchemes(), so every scheme appears
    for (s in names(gdsCloudSchemes()))
        expect_match(err, paste0(s, "://"), fixed = TRUE)
})

test_that("gdsCloudOpen accepts every scheme reported by gdsCloudSchemes", {
    # a valid scheme must fail with a network/access error, not a scheme error
    for (s in names(gdsCloudSchemes())) {
        msg <- tryCatch(
            gdsCloudOpen(paste0(s, "://invalid.test/nonexistent.gds")),
            error = function(e) conditionMessage(e))
        expect_false(grepl("Unsupported URL scheme", msg))
    }
})
