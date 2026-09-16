# Azure Shared Key handling that can be checked without network access

.with_azure_creds <- function(code)
{
    env <- get(".gdscloud_env", envir = asNamespace("gdscloud"),
        inherits = FALSE)
    old <- mget(c("azure_account_name", "azure_account_key",
        "azure_sas_token"), envir = env, ifnotfound = list(NULL))
    on.exit(for (nm in names(old)) assign(nm, old[[nm]], envir = env),
        add = TRUE)
    force(code)
}

test_that("an invalid base64 account key is rejected before any request", {
    .with_azure_creds({
        gdsCloudConfigAzure(account_name = "acct", account_key = "not base64!!",
            sas_token = "")
        expect_error(gdsCloudOpen("az://container/file.gds"), "base64")
    })
})

test_that("an over-long account key is rejected instead of overflowing", {
    .with_azure_creds({
        # 1000 valid base64 characters decode to 750 bytes, more than any
        # real key (64 bytes) and more than the decode buffer
        long_key <- paste(rep("QUJD", 250), collapse = "")
        gdsCloudConfigAzure(account_name = "acct", account_key = long_key,
            sas_token = "")
        expect_error(gdsCloudOpen("az://container/file.gds"), "base64")
    })
})

test_that("a well-formed key passes validation (fails later on the network)", {
    .with_azure_creds({
        key <- "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        gdsCloudConfigAzure(account_name = "acct", account_key = key,
            sas_token = "")
        err <- tryCatch(gdsCloudOpen("az://container/file.gds"),
            error = function(e) conditionMessage(e))
        expect_false(grepl("base64", err))
    })
})
