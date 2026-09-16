# Request signing, checked without network access via the internal
# .prepare_request() hook (the request a provider would send).

.with_creds <- function(code)
{
    env <- get(".gdscloud_env", envir = asNamespace("gdscloud"),
        inherits = FALSE)
    nms <- c("aws_access_key_id", "aws_secret_access_key", "aws_region",
        "aws_session_token", "aws_endpoint", "aws_path_style",
        "azure_account_name", "azure_account_key",
        "azure_sas_token", "azure_access_token", "azure_endpoint_suffix",
        "azure_endpoint", "gcs_access_token", "http_bearer_token")
    old <- mget(nms, envir = env, ifnotfound = list(NULL))
    on.exit(for (nm in nms) assign(nm, old[[nm]], envir = env), add = TRUE)
    # scrub environment variables that would otherwise leak in
    ev <- c("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
        "AWS_SESSION_TOKEN", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3",
        "GDSCLOUD_S3_PATH_STYLE", "AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY",
        "AZURE_STORAGE_SAS_TOKEN", "AZURE_STORAGE_ACCESS_TOKEN",
        "AZURE_STORAGE_ENDPOINT_SUFFIX", "AZURE_STORAGE_SERVICE_ENDPOINT",
        "AZURE_STORAGE_CONNECTION_STRING", "GCS_ACCESS_TOKEN",
        "GDSCLOUD_HTTP_TOKEN")
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

test_that("S3 custom endpoints: URL forms and the signed Host header", {
    .with_creds({
        cfg <- function(...) gdsCloudConfigS3(aws_access_key_id = "K",
            aws_secret_access_key = "S", region = "us-east-1",
            session_token = "", ...)
        # path style is the default for a custom endpoint; port kept
        cfg(endpoint = "http://minio.local:9000", path_style = NA)
        r <- gdscloud:::.prepare_request("s3://bkt/dir/f.gds")
        expect_equal(r$url, "http://minio.local:9000/bkt/dir/f.gds")
        # scheme defaults to https; explicit virtual-hosted style
        cfg(endpoint = "s3.wasabisys.com", path_style = FALSE)
        r <- gdscloud:::.prepare_request("s3://bkt/f.gds")
        expect_equal(r$url, "https://bkt.s3.wasabisys.com/f.gds")
        # a path prefix on the endpoint, trailing slash and query ignored
        cfg(endpoint = "https://gw.example.org/s3/?x=1", path_style = TRUE)
        r <- gdscloud:::.prepare_request("s3://bkt/f.gds")
        expect_equal(r$url, "https://gw.example.org/s3/bkt/f.gds")
        # path style on Amazon S3 itself (bucket names with dots)
        cfg(endpoint = "", path_style = TRUE)
        r <- gdscloud:::.prepare_request("s3://my.bucket/f.gds")
        expect_equal(r$url, "https://s3.us-east-1.amazonaws.com/my.bucket/f.gds")
        # invalid endpoint
        cfg(endpoint = "https:///nohost", path_style = NA)
        expect_error(gdscloud:::.prepare_request("s3://bkt/f.gds"),
            "invalid S3 endpoint")
        expect_error(gdsCloudConfigS3(endpoint = 1), "endpoint")
        expect_error(gdsCloudConfigS3(path_style = "yes"), "path_style")

        # the signature covers host:port and the path-style URI
        skip_if_not_installed("openssl")
        cfg(endpoint = "http://minio.local:9000", path_style = NA)
        r <- gdscloud:::.prepare_request("s3://bkt/dir/f.gds",
            range = "bytes=0-9",
            time = as.POSIXct("2013-05-24 00:00:00", tz = "UTC"))
        auth <- grep("^Authorization:", r$headers, value = TRUE)
        sha <- function(x) openssl::sha256(charToRaw(x))
        hmac <- function(key, x) openssl::sha256(charToRaw(x), key = key)
        hex <- function(x) paste(unclass(x), collapse = "")
        empty <- paste0("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca4959",
            "91b7852b855")
        canonical <- paste("GET", "/bkt/dir/f.gds", "",
            "host:minio.local:9000", "range:bytes=0-9",
            paste0("x-amz-content-sha256:", empty),
            "x-amz-date:20130524T000000Z", "",
            "host;range;x-amz-content-sha256;x-amz-date", empty, sep = "\n")
        sts <- paste("AWS4-HMAC-SHA256", "20130524T000000Z",
            "20130524/us-east-1/s3/aws4_request", hex(sha(canonical)),
            sep = "\n")
        k <- hmac(charToRaw("AWS4S"), "20130524")
        k <- hmac(k, "us-east-1"); k <- hmac(k, "s3"); k <- hmac(k, "aws4_request")
        expect_equal(sub(".*Signature=", "", auth), hex(hmac(k, sts)))
    })
})

test_that("S3 endpoint and path style resolve from env vars and URL entries", {
    .with_creds({
        env <- get(".gdscloud_env", envir = asNamespace("gdscloud"))
        old <- env$url_credentials
        on.exit(env$url_credentials <- old, add = TRUE)
        env$url_credentials <- list()
        gdsCloudConfigS3(endpoint = "", path_style = NA)

        withr::local_envvar(AWS_ENDPOINT_URL = "https://env.example.org",
            GDSCLOUD_S3_PATH_STYLE = "false")
        cred <- gdscloud:::.get_s3_credentials("s3://b/k")
        expect_equal(cred$endpoint, "https://env.example.org")
        expect_false(cred$path_style)

        withr::local_envvar(AWS_ENDPOINT_URL_S3 = "https://s3env.example.org")
        expect_equal(gdscloud:::.get_s3_credentials("s3://b/k")$endpoint,
            "https://s3env.example.org")

        gdsCloudConfigS3(endpoint = "https://global.example.org",
            path_style = TRUE)
        cred <- gdscloud:::.get_s3_credentials("s3://b/k")
        expect_equal(cred$endpoint, "https://global.example.org")
        expect_true(cred$path_style)

        gdsCloudConfigS3(endpoint = "https://r2.example.org",
            url = "s3://r2bucket/")
        expect_equal(gdscloud:::.get_s3_credentials("s3://r2bucket/k")$endpoint,
            "https://r2.example.org")
        expect_equal(gdscloud:::.get_s3_credentials("s3://other/k")$endpoint,
            "https://global.example.org")
        # unset again (the entry is removed when all fields are NULL)
        gdsCloudConfigS3(url = "s3://r2bucket/")
    })
})

test_that("Azure bearer token, endpoint suffix and custom endpoint", {
    .with_creds({
        cfg <- function(...) {
            gdsCloudConfigAzure(account_name = "acct", account_key = "",
                sas_token = "", access_token = "", endpoint_suffix = "",
                endpoint = "")
            gdsCloudConfigAzure(...)
        }
        # OAuth2: bearer header plus the mandatory x-ms-version
        cfg(access_token = "eyJ.tok")
        r <- gdscloud:::.prepare_request("az://cont/b.gds")
        expect_equal(r$url, "https://acct.blob.core.windows.net/cont/b.gds")
        expect_setequal(r$headers,
            c("Authorization: Bearer eyJ.tok", "x-ms-version: 2020-10-02"))
        # a SAS token takes precedence over the bearer token
        cfg(access_token = "eyJ.tok", sas_token = "sv=1&sig=s")
        r <- gdscloud:::.prepare_request("az://cont/b.gds")
        expect_equal(r$url,
            "https://acct.blob.core.windows.net/cont/b.gds?sv=1&sig=s")
        expect_length(r$headers, 0)
        # sovereign cloud suffix
        cfg(endpoint_suffix = "blob.core.chinacloudapi.cn")
        r <- gdscloud:::.prepare_request("az://cont/b.gds")
        expect_equal(r$url, "https://acct.blob.core.chinacloudapi.cn/cont/b.gds")
        # Azurite-style endpoint with the account in the path
        cfg(endpoint = "http://127.0.0.1:10000/devstoreaccount1/")
        r <- gdscloud:::.prepare_request("az://cont/b.gds")
        expect_equal(r$url, "http://127.0.0.1:10000/devstoreaccount1/cont/b.gds")
        cfg(endpoint = "https:///nohost")
        expect_error(gdscloud:::.prepare_request("az://cont/b.gds"),
            "invalid Azure endpoint")
        expect_error(gdsCloudConfigAzure(endpoint = 1), "endpoint")

        # Shared Key against Azurite: the canonicalized resource includes
        # the account twice (once as the account, once from the URL path)
        skip_if_not_installed("openssl")
        key <- paste0("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6",
            "tq/K1SZFPTOtr/KBHBeksoGMGw==")
        gdsCloudConfigAzure(account_name = "devstoreaccount1", account_key = key,
            sas_token = "", access_token = "",
            endpoint = "http://127.0.0.1:10000/devstoreaccount1")
        r <- gdscloud:::.prepare_request("az://cont/blob.gds",
            range = "bytes=0-9",
            time = as.POSIXct("2013-05-24 00:00:00", tz = "UTC"))
        auth <- grep("^Authorization:", r$headers, value = TRUE)
        sts <- paste0("GET\n\n\n\n\n\n\n\n\n\n\nbytes=0-9\n",
            "x-ms-date:Fri, 24 May 2013 00:00:00 GMT\nx-ms-version:2020-10-02\n",
            "/devstoreaccount1/devstoreaccount1/cont/blob.gds")
        sig <- openssl::sha256(charToRaw(sts), key = openssl::base64_decode(key))
        expect_equal(auth, paste0("Authorization: SharedKey devstoreaccount1:",
            openssl::base64_encode(sig)))
    })
})

test_that("Azure credentials resolve from env vars and the connection string", {
    .with_creds({
        gdsCloudConfigAzure(account_name = "", account_key = "", sas_token = "",
            access_token = "", endpoint_suffix = "", endpoint = "")
        withr::local_envvar(AZURE_STORAGE_ACCESS_TOKEN = "envtok",
            AZURE_STORAGE_ENDPOINT_SUFFIX = "blob.core.usgovcloudapi.net",
            AZURE_STORAGE_SERVICE_ENDPOINT = "https://svc.example.org")
        cred <- gdscloud:::.get_azure_credentials("az://c/b")
        expect_equal(cred$access_token, "envtok")
        expect_equal(cred$endpoint_suffix, "blob.core.usgovcloudapi.net")
        expect_equal(cred$endpoint, "https://svc.example.org")

        # connection string as the last resort
        withr::local_envvar(AZURE_STORAGE_ACCESS_TOKEN = NA,
            AZURE_STORAGE_ENDPOINT_SUFFIX = NA,
            AZURE_STORAGE_SERVICE_ENDPOINT = NA,
            AZURE_STORAGE_CONNECTION_STRING = paste0(
                "DefaultEndpointsProtocol=https;AccountName=csacct;",
                "AccountKey=Y3NrZXk=;EndpointSuffix=core.chinacloudapi.cn"))
        cred <- gdscloud:::.get_azure_credentials("az://c/b")
        expect_equal(cred$account_name, "csacct")
        expect_equal(cred$account_key, "Y3NrZXk=")
        expect_equal(cred$endpoint_suffix, "blob.core.chinacloudapi.cn")
        expect_equal(cred$endpoint, "")

        withr::local_envvar(AZURE_STORAGE_CONNECTION_STRING =
            "UseDevelopmentStorage=true")
        cred <- gdscloud:::.get_azure_credentials("az://c/b")
        expect_equal(cred$account_name, "devstoreaccount1")
        expect_equal(cred$endpoint, "http://127.0.0.1:10000/devstoreaccount1")
        expect_match(cred$account_key, "^Eby8vdM02x")

        # explicit settings win over the connection string
        gdsCloudConfigAzure(account_name = "explicit")
        expect_equal(gdscloud:::.get_azure_credentials("az://c/b")$account_name,
            "explicit")
        expect_equal(gdscloud:::.azure_connection_string("")$account_name, "")
    })
})
