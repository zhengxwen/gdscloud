# Timeouts and user interrupts, using a TCP server that accepts
# connections but never answers.

skip_if_not_installed("callr")

start_silent_server <- function()
{
    port <- httpuv::randomPort()
    proc <- callr::r_bg(function(port) {
        s <- serverSocket(port)
        on.exit(close(s))
        repeat {
            con <- socketAccept(s, blocking = TRUE)
            Sys.sleep(120)   # keep the connection open, send nothing
            close(con)
        }
    }, args = list(port = port))
    # wait until the port accepts connections
    ok <- FALSE
    for (i in seq_len(100)) {
        if (!proc$is_alive()) break
        ok <- tryCatch({
            con <- socketConnection("127.0.0.1", port, blocking = TRUE,
                timeout = 1)
            close(con)
            TRUE
        }, error = function(e) FALSE, warning = function(w) FALSE)
        if (ok) break
        Sys.sleep(0.1)
    }
    if (!ok) { proc$kill(); skip("silent TCP server did not start") }
    list(url = sprintf("http://127.0.0.1:%d/file.gds", port), proc = proc)
}

test_that("a stalled server hits the low-speed timeout instead of hanging", {
    skip_if_not_installed("httpuv")
    srv <- start_silent_server()
    on.exit(srv$proc$kill(), add = TRUE)
    old <- gdsCloudOptions()
    on.exit(do.call(gdsCloudOptions, old), add = TRUE)
    gdsCloudOptions(timeout = 1)
    t0 <- Sys.time()
    err <- tryCatch(gdsCloudOpen(srv$url),
        error = function(e) conditionMessage(e))
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    expect_match(err, "curl error")
    expect_lt(elapsed, 15)
})

test_that("a transfer can be interrupted with SIGINT", {
    skip_if_not_installed("httpuv")
    skip_on_os("windows")
    srv <- start_silent_server()
    on.exit(srv$proc$kill(), add = TRUE)
    # run the (stalling) open in a child R process and interrupt it
    child <- callr::r_bg(function(url) {
        library(gdscloud)
        gdsCloudOptions(timeout = 60)
        tryCatch(gdsCloudOpen(url), error = function(e) conditionMessage(e))
    }, args = list(url = srv$url))
    on.exit(child$kill(), add = TRUE)
    Sys.sleep(3)   # let the child get into the transfer
    child$interrupt()
    child$wait(timeout = 15000)
    expect_false(child$is_alive())
    expect_match(child$get_result(), "interrupted by the user")
})
