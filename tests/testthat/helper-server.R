# Local HTTP test server (httpuv in a background R process via callr),
# used to exercise the range-read code path without network access.
#
# Routes, all serving files from `dir`:
#   /__ping            readiness probe
#   /data/<f>          proper HTTP Range support (206 + Content-Range)
#   /norange/<f>       ignores Range: always 200 with the whole file
#   /badrange/<f>      206 whose Content-Range start is always 0
#   /flaky/<f>         503 on two out of every three requests, else /data
#   /auth/<f>          requires "Authorization: Bearer secret-token"

.test_server_app <- function(dir)
{
    counter <- 0L
    resp <- function(status, body, headers = list())
    {
        if (is.character(body)) body <- charToRaw(body)
        headers[["Content-Type"]] <- "application/octet-stream"
        list(status = status, headers = headers, body = body)
    }
    serve <- function(req, fname, mode)
    {
        path <- file.path(dir, basename(fname))
        if (!file.exists(path))
            return(resp(404L, "<Error>NoSuchKey</Error>"))
        size <- file.size(path)
        rng <- req$HTTP_RANGE
        con <- file(path, "rb")
        on.exit(close(con))
        if (mode == "norange" || is.null(rng) ||
            !grepl("^bytes=[0-9]+-[0-9]*$", rng))
        {
            body <- readBin(con, "raw", size)
            return(resp(200L, body, list(`Accept-Ranges` = "bytes",
                `Content-Length` = as.character(size))))
        }
        m <- regmatches(rng, regexec("^bytes=([0-9]+)-([0-9]*)$", rng))[[1L]]
        start <- as.numeric(m[2L])
        end <- if (nzchar(m[3L])) min(as.numeric(m[3L]), size - 1) else size - 1
        if (start >= size)
            return(resp(416L, "", list(`Content-Range` =
                sprintf("bytes */%.0f", size))))
        seek(con, start)
        body <- readBin(con, "raw", end - start + 1)
        cr_start <- if (mode == "badrange") 0 else start
        resp(206L, body, list(`Accept-Ranges` = "bytes",
            `Content-Range` = sprintf("bytes %.0f-%.0f/%.0f", cr_start,
                cr_start + (end - start), size)))
    }
    function(req)
    {
        counter <<- counter + 1L
        p <- req$PATH_INFO
        if (p == "/__ping") return(resp(200L, "ok"))
        parts <- strsplit(sub("^/", "", p), "/", fixed = TRUE)[[1L]]
        if (length(parts) != 2L) return(resp(404L, "not found"))
        mode <- parts[1L]; fname <- parts[2L]
        if (mode %in% c("data", "norange", "badrange"))
            serve(req, fname, mode)
        else if (mode == "auth")
        {
            if (identical(req$HTTP_AUTHORIZATION, "Bearer secret-token"))
                serve(req, fname, "data")
            else
                resp(401L, "{\"error\":\"unauthorized\"}")
        }
        else if (mode == "flaky")
        {
            if (counter %% 3L != 0L)
                resp(503L, "<Error><Code>SlowDown</Code></Error>")
            else
                serve(req, fname, "data")
        }
        else
            resp(404L, "not found")
    }
}

# Start the server; returns list(url=<base url>, proc=<callr process>)
start_test_server <- function(dir)
{
    port <- httpuv::randomPort()
    app <- .test_server_app
    proc <- callr::r_bg(function(app, dir, port) {
        httpuv::runServer("127.0.0.1", port, list(call = app(dir)))
    }, args = list(app = app, dir = dir, port = port))
    base <- sprintf("http://127.0.0.1:%d", port)
    ping <- function() {
        con <- url(paste0(base, "/__ping"))
        on.exit(close(con))
        identical(readLines(con, warn = FALSE), "ok")
    }
    ok <- FALSE
    for (i in seq_len(100)) {
        if (!proc$is_alive()) break
        ok <- tryCatch(ping(), error = function(e) FALSE,
            warning = function(w) FALSE)
        if (ok) break
        Sys.sleep(0.1)
    }
    if (!ok) {
        err <- tryCatch(proc$read_all_error(), error = function(e) "")
        proc$kill()
        stop("test HTTP server did not start: ", err)
    }
    list(url = base, proc = proc)
}

stop_test_server <- function(srv)
{
    if (!is.null(srv$proc) && srv$proc$is_alive()) srv$proc$kill()
    invisible()
}

# Create a small GDS file spanning several 1 MB cache blocks
make_test_gds <- function(path)
{
    set.seed(1000)
    f <- gdsfmt::createfn.gds(path)
    gdsfmt::add.gdsn(f, "sample.id", paste0("S", 1:50))
    gdsfmt::add.gdsn(f, "position", as.integer(round(runif(5000, 1, 1e8))),
        compress = "LZMA_RA", closezip = TRUE)
    # ~3 MB of incompressible data so reads cross block boundaries
    gdsfmt::add.gdsn(f, "geno", matrix(sample.int(200L, 1.5e6, TRUE),
        nrow = 500), storage = "int32")
    gdsfmt::add.gdsn(f, "text", rep(c("alpha", "beta", "gamma"), 300),
        compress = "ZIP_RA", closezip = TRUE)
    gdsfmt::closefn.gds(f)
    invisible(path)
}
