# Request signing, checked without network access via the internal
# .prepare_request() hook (the request a provider would send).

.with_creds <- function(code)
{
    env <- get(".gdscloud_env", envir = asNamespace("gdscloud"),
        inherits = FALSE)
    nms <- c("aws_access_key_id", "aws_secret_access_key", "aws_region",
        "aws_session_token", "azure_account_name", "azure_account_key",
        "azure_sas_token", "gcs_access_token", "http_bearer_token")
    old <- mget(nms, envir = env, ifnotfound = list(NULL))
    on.exit(for (nm in nms) assign(nm, old[[nm]], envir = env), add = TRUE)
    # scrub environment variables that would otherwise leak in
    ev <- c("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
        "AWS_SESSION_TOKEN", "AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY",
        "AZURE_STORAGE_SAS_TOKEN", "GCS_ACCESS_TOKEN", "GDSCLOUD_HTTP_TOKEN")
    withr::local_envvar(setNames(as.list(rep(NA_character_, length(ev))), ev))
    force(code)
}

test_that("AWS SigV4 matches the GET Object example of the S3 API reference", {
    .with_creds({
        # https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
        # (example "GET Object": examplebucket, us-east-1, 2013-05-24,
        # Range: bytes=0-9). The reference signs for the legacy global host
        # examplebucket.s3.amazonaws.com; gdscloud uses the regional host,
        # which changes the 'host' canonical header, so the signature is
        # verified below by recomputing it independently for that host.
        gdsCloudConfigS3(
            aws_access_key_id = "AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region = "us-east-1", session_token = "")
        r <- gdscloud:::.prepare_request("s3://examplebucket/test.txt",
            range = "bytes=0-9",
            time = as.POSIXct("2013-05-24 00:00:00", tz = "UTC"))
        expect_equal(r$url,
            "https://examplebucket.s3.us-east-1.amazonaws.com/test.txt")
        auth <- grep("^Authorization:", r$headers, value = TRUE)
        expect_match(auth, paste0("^Authorization: AWS4-HMAC-SHA256 ",
            "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, ",
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, ",
            "Signature=[0-9a-f]{64}$"))
        expect_true("x-amz-date: 20130524T000000Z" %in% r$headers)
        expect_true(paste0("x-amz-content-sha256: e3b0c44298fc1c149afbf4c89",
            "96fb92427ae41e4649b934ca495991b7852b855") %in% r$headers)
        expect_false(any(grepl("security-token", r$headers)))

        # the signature itself, recomputed independently in R for the
        # regional host (needs the 'openssl' package)
        skip_if_not_installed("openssl")
        sha <- function(x) openssl::sha256(charToRaw(x))
        hmac <- function(key, x) openssl::sha256(charToRaw(x), key = key)
        hex <- function(x) paste(unclass(x), collapse = "")
        canonical <- paste(
            "GET", "/test.txt", "",
            "host:examplebucket.s3.us-east-1.amazonaws.com",
            "range:bytes=0-9",
            paste0("x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb924",
                "27ae41e4649b934ca495991b7852b855"),
            "x-amz-date:20130524T000000Z", "",
            "host;range;x-amz-content-sha256;x-amz-date",
            paste0("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca4959",
                "91b7852b855"),
            sep = "\n")
        sts <- paste("AWS4-HMAC-SHA256", "20130524T000000Z",
            "20130524/us-east-1/s3/aws4_request", hex(sha(canonical)),
            sep = "\n")
        k <- hmac(charToRaw("AWS4wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            "20130524")
        k <- hmac(k, "us-east-1"); k <- hmac(k, "s3"); k <- hmac(k, "aws4_request")
        expect_equal(sub(".*Signature=", "", auth), hex(hmac(k, sts)))
    })
})

test_that("AWS SigV4 signs the session token when one is configured", {
    .with_creds({
        gdsCloudConfigS3(aws_access_key_id = "AKIA", aws_secret_access_key = "s",
            region = "eu-west-1", session_token = "TOKEN")
        r <- gdscloud:::.prepare_request("s3://b/k.gds", range = "bytes=0-0")
        expect_true("x-amz-security-token: TOKEN" %in% r$headers)
        expect_match(grep("^Authorization:", r$headers, value = TRUE),
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date;x-amz-security-token,")
        expect_match(grep("^Authorization:", r$headers, value = TRUE),
            "/eu-west-1/s3/aws4_request")
    })
})

test_that("object keys are percent-encoded once in the request path", {
    .with_creds({
        gdsCloudConfigS3(aws_access_key_id = "", aws_secret_access_key = "",
            region = "us-east-1", session_token = "")
        r <- gdscloud:::.prepare_request("s3://my-bucket/dir with space/f+g%.gds")
        expect_equal(r$url, paste0("https://my-bucket.s3.us-east-1.amazonaws.com/",
            "dir%20with%20space/f%2Bg%25.gds"))
        expect_length(r$headers, 0)   # anonymous: no auth headers

        gdsCloudConfigGCS(access_token = "tok")
        r <- gdscloud:::.prepare_request("gs://bkt/a b/c.gds")
        expect_equal(r$url, "https://storage.googleapis.com/bkt/a%20b/c.gds")
        expect_equal(r$headers, "Authorization: Bearer tok")

        gdsCloudConfigAzure(account_name = "acct", account_key = "",
            sas_token = "?sv=2020&sig=xyz")
        r <- gdscloud:::.prepare_request("az://cont/a b.gds")
        expect_equal(r$url,
            "https://acct.blob.core.windows.net/cont/a%20b.gds?sv=2020&sig=xyz")
        expect_length(r$headers, 0)
    })
})

test_that("Azure Shared Key requests carry the expected headers", {
    .with_creds({
        key <- "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        gdsCloudConfigAzure(account_name = "acct", account_key = key,
            sas_token = "")
        r <- gdscloud:::.prepare_request("az://cont/blob.gds", range = "bytes=0-9",
            time = as.POSIXct("2013-05-24 00:00:00", tz = "UTC"))
        expect_equal(r$url, "https://acct.blob.core.windows.net/cont/blob.gds")
        expect_true("x-ms-date: Fri, 24 May 2013 00:00:00 GMT" %in% r$headers)
        expect_true("x-ms-version: 2020-10-02" %in% r$headers)
        auth <- grep("^Authorization:", r$headers, value = TRUE)
        expect_match(auth, "^Authorization: SharedKey acct:[A-Za-z0-9+/]{43}=$")

        # independent recomputation of the signature
        skip_if_not_installed("openssl")
        sts <- paste0("GET\n\n\n\n\n\n\n\n\n\n\nbytes=0-9\n",
            "x-ms-date:Fri, 24 May 2013 00:00:00 GMT\nx-ms-version:2020-10-02\n",
            "/acct/cont/blob.gds")
        sig <- openssl::sha256(charToRaw(sts), key = openssl::base64_decode(key))
        expect_equal(sub(".*:", "", auth), openssl::base64_encode(sig))
    })
})

test_that("HTTP requests carry the Bearer token only when configured", {
    .with_creds({
        gdsCloudConfigHTTP(bearer_token = "")
        r <- gdscloud:::.prepare_request("https://example.org/x.gds")
        expect_equal(r$url, "https://example.org/x.gds")
        expect_length(r$headers, 0)
        gdsCloudConfigHTTP(bearer_token = "t0k")
        r <- gdscloud:::.prepare_request("https://example.org/x.gds")
        expect_equal(r$headers, "Authorization: Bearer t0k")
    })
})
