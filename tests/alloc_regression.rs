//! Allocation regression test.
//!
//! This is the load-bearing test of the whole design. Without it, "zero
//! allocations on the steady-state path" is an assertion in a design document
//! that decays on the first careless commit.
//!
//! The counting allocator needs `unsafe` — `GlobalAlloc` is an unsafe trait — so
//! it lives here, in a test target, rather than in the crate. `armature-h1`
//! itself keeps `#![forbid(unsafe_code)]` intact.

use armature_h1::{ConnConfig, Connection, DateCache, Limits, Request, Response};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Per-thread rather than process-wide. `cargo test` runs the test functions on
// separate threads concurrently, and a shared static counter attributes every
// other test's allocations to whichever test happens to be armed — which showed
// up as an identical, load-independent count in all four budgets.
//
// `const`-initialized so the TLS slot needs no lazy-init check and registers no
// destructor: both would themselves allocate, from inside the allocator.
thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

/// Count one allocation against the current thread, if it is armed.
///
/// `try_with` rather than `with`: during TLS teardown the slot is gone, and a
/// panic from inside `GlobalAlloc` would abort the process.
fn tick() {
    let armed = COUNTING.try_with(Cell::get).unwrap_or(false);
    if armed {
        let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
    }
}

/// The system allocator, counting allocations while armed.
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        tick();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        tick();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        tick();
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Arm the counter, resetting it to zero.
fn arm() {
    ALLOCS.with(|c| c.set(0));
    COUNTING.with(|c| c.set(true));
}

/// Disarm the counter and report what it saw.
///
/// The counter is armed around `await` points on a current-thread runtime, so it
/// counts everything this thread does in the window — including the server task
/// the runtime polls in between. That is the intent: the number covers the whole
/// request path, not just the client half.
fn disarm() -> u64 {
    COUNTING.with(|c| c.set(false));
    ALLOCS.with(Cell::get)
}

async fn hello(_req: Request) -> Response {
    Response::text("hi")
}

async fn drain(mut req: Request) -> Response {
    let _ = req.body.collect(64 * 1024).await;
    Response::text("hi")
}

fn limits() -> Limits {
    Limits {
        idle_timeout: Duration::from_millis(200),
        header_timeout: Duration::from_millis(200),
        ..Default::default()
    }
}

/// Serve `count` copies of `request` on one keep-alive connection, returning the
/// allocations attributable to the steady state.
///
/// `warm` requests run first with counting disarmed, so one-time costs — buffer
/// growth and sizing, the task allocation, tokio's timer registration — are
/// excluded. What remains is per-request cost, which is the number that must not
/// grow.
fn steady_state_allocs<S, Fut>(request: &'static [u8], service: S, warm: usize, count: usize) -> u64
where
    S: Fn(Request) -> Fut + Copy + 'static,
    Fut: std::future::Future<Output = Response> + 'static,
{
    steady_state_allocs_with_peer(request, service, warm, count, None)
}

/// As [`steady_state_allocs`], with the connection's peer address populated.
///
/// Split out rather than folded into every call site because `None` is the
/// shape all the other cases measure; the point of the parameter is that one
/// case measures the other shape.
fn steady_state_allocs_with_peer<S, Fut>(
    request: &'static [u8],
    service: S,
    warm: usize,
    count: usize,
    peer: Option<SocketAddr>,
) -> u64
where
    S: Fn(Request) -> Fut + Copy + 'static,
    Fut: std::future::Future<Output = Response> + 'static,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    // `Connection::new` allocates its reusable `Sleep`, which requires a runtime
    // context. In the real server this happens inside the worker runtime; here it
    // has to be entered explicitly.
    let _guard = rt.enter();

    let local = tokio::task::LocalSet::new();
    let (mut client, server) = tokio::io::duplex(1024 * 1024);

    let conn = Connection::new(
        server,
        service,
        Rc::new(ConnConfig {
            limits: limits(),
            tick: Duration::from_millis(50),
            server_name: None,
        }),
        Rc::new(RefCell::new(DateCache::new())),
    )
    .with_peer(peer);
    // Spawned on the LocalSet handle before `run_until`, matching the pattern the
    // connection tests use. Spawning from inside the `run_until` future instead
    // left the server task unscheduled and the whole harness wedged.
    let task = local.spawn_local(conn.serve());

    rt.block_on(local.run_until(async move {
        let mut scratch = vec![0u8; 64 * 1024];

        for i in 0..warm {
            client.write_all(request).await.expect("write");
            match tokio::time::timeout(
                Duration::from_secs(5),
                read_one_response(&mut client, &mut scratch),
            )
            .await
            {
                Ok(()) => {}
                Err(_) => panic!("warm-up request {i} never got a response"),
            }
        }

        // Counting is armed around the awaits directly. Driving the loop through a
        // nested inline executor would keep the harness out of the count but
        // deadlocks, because spinning on this thread starves the very LocalSet
        // that has to poll the server task.
        arm();
        for _ in 0..count {
            client.write_all(request).await.expect("write");
            read_one_response(&mut client, &mut scratch).await;
        }
        let allocs = disarm();

        drop(client);
        let _ = task.await;
        allocs
    }))
}

/// Read exactly one response off `client`.
async fn read_one_response(client: &mut tokio::io::DuplexStream, scratch: &mut [u8]) {
    // Every response in this test is small and written in one go, so one read
    // suffices; a partial read would show up as a hang rather than a wrong count.
    let n = client.read(scratch).await.expect("read");
    assert!(n > 0, "server closed unexpectedly");
    assert!(
        scratch[..n].starts_with(b"HTTP/1.1 200 OK"),
        "unexpected response: {}",
        String::from_utf8_lossy(&scratch[..n.min(120)])
    );
}

const KEEPALIVE_GET: &[u8] = b"GET / HTTP/1.1\r\nHost: a.example\r\n\r\n";

const BROWSER_GET: &[u8] = b"GET /index.html HTTP/1.1\r\n\
Host: a.example\r\n\
User-Agent: Mozilla/5.0\r\n\
Accept: text/html,application/xhtml+xml\r\n\
Accept-Language: en-US,en;q=0.9\r\n\
Accept-Encoding: gzip, deflate, br\r\n\
Connection: keep-alive\r\n\
Cache-Control: max-age=0\r\n\
\r\n";

// Mirrors `BROWSER_GET` but with the mixed-case unknown headers a real browser
// actually sends (Sec-Fetch-*, Sec-Ch-Ua*, Upgrade-Insecure-Requests, DNT).
// `HeaderId::from_bytes` (src/parse.rs) only recognizes well-known names; an
// unrecognized name takes the `HeaderId::Other` branch, which lowercases into
// a fresh allocation whenever the name has an uppercase byte. `BROWSER_GET`'s
// header set is entirely well-known names, so it never exercises that branch
// and reports zero. This fixture is the honest one for "GET with N browser
// headers."
const BROWSER_GET_MIXED_CASE: &[u8] = b"GET /index.html HTTP/1.1\r\n\
Host: a.example\r\n\
User-Agent: Mozilla/5.0\r\n\
Accept: text/html,application/xhtml+xml\r\n\
Accept-Language: en-US,en;q=0.9\r\n\
Accept-Encoding: gzip, deflate, br\r\n\
Connection: keep-alive\r\n\
Sec-Fetch-Mode: navigate\r\n\
Sec-Fetch-Site: none\r\n\
Sec-Fetch-Dest: document\r\n\
Sec-Ch-Ua: \"Not.A/Brand\";v=\"8\"\r\n\
Upgrade-Insecure-Requests: 1\r\n\
DNT: 1\r\n\
\r\n";

const FIXED_BODY_POST: &[u8] =
    b"POST / HTTP/1.1\r\nHost: a.example\r\nContent-Length: 5\r\n\r\nhello";

const CHUNKED_POST: &[u8] = b"POST / HTTP/1.1\r\nHost: a.example\r\n\
Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";

/// The per-request allocation budget: zero.
///
/// An exact zero rather than a threshold, because a threshold is a slow leak
/// waiting to be tolerated. Raising this number requires justifying in the commit
/// message what the crate bought in exchange.
const BUDGET_PER_GET: u64 = 0;

#[test]
fn steady_state_keepalive_get_stays_within_budget() {
    let n = 100;
    let allocs = steady_state_allocs(KEEPALIVE_GET, hello, 50, n);
    let per_request = allocs as f64 / n as f64;
    println!("keep-alive GET: {allocs} allocations over {n} requests ({per_request:.2}/request)");
    assert!(
        allocs <= BUDGET_PER_GET * n as u64,
        "keep-alive GET budget exceeded: {per_request:.2} allocations per request, \
         budget is {BUDGET_PER_GET}"
    );
}

/// The same case, with the connection's peer address populated.
///
/// Every other case here builds its connection with `Connection::new` alone,
/// which leaves `peer` as `None` — so the field is only ever measured empty,
/// and an empty `Option` costs nothing however its payload is represented.
/// `SocketAddr` is `Copy` and rides inline in the `Request`, so filling it in
/// must not move the count off zero. The day someone reaches for an
/// `Rc<String>` or a formatted address, this is the row that fails.
#[test]
fn steady_state_keepalive_get_with_peer_stays_within_budget() {
    let n = 100;
    let peer: SocketAddr = "203.0.113.7:54321".parse().expect("addr");
    let allocs = steady_state_allocs_with_peer(KEEPALIVE_GET, hello, 50, n, Some(peer));
    let per_request = allocs as f64 / n as f64;
    println!(
        "keep-alive GET with peer: {allocs} allocations over {n} requests ({per_request:.2}/request)"
    );
    assert!(
        allocs <= BUDGET_PER_GET * n as u64,
        "a populated peer address must not cost an allocation: {per_request:.2} per request, \
         budget is {BUDGET_PER_GET}"
    );
}

#[test]
fn browser_sized_get_stays_within_budget() {
    let n = 100;
    let allocs = steady_state_allocs(BROWSER_GET, hello, 50, n);
    let per_request = allocs as f64 / n as f64;
    println!("browser GET (7 headers): {allocs} over {n} ({per_request:.2}/request)");
    // Seven headers must cost no more than one, since HeaderVec keeps 16 inline
    // slots and every value is a slice of the read buffer. If this scales with
    // header count, the zero-copy projection has regressed into copying.
    assert!(
        allocs <= BUDGET_PER_GET * n as u64,
        "header count must not drive allocations: {per_request:.2} per request"
    );
}

/// Six mixed-case unknown header names (Sec-Fetch-Mode, Sec-Fetch-Site,
/// Sec-Fetch-Dest, Sec-Ch-Ua, Upgrade-Insecure-Requests, DNT), each taking the
/// lowercasing-copy branch in `finish_parse_head` (src/parse.rs, ~line 235)
/// because `HeaderId::from_bytes` does not recognize them and each contains an
/// uppercase byte. That is one allocation per unknown mixed-case header, per
/// request: six here. `browser_sized_get_stays_within_budget` above stays at
/// zero only because every one of its seven header names is well-known and
/// never reaches that branch.
///
/// This budget is deliberately nonzero — see the module doc on
/// `BUDGET_PER_GET` for why a nonzero number still gets pinned exactly rather
/// than bounded by a threshold.
const BUDGET_PER_MIXED_CASE_HEADER_GET: u64 = 6;

#[test]
fn browser_sized_get_with_mixed_case_unknown_headers_stays_within_budget() {
    let n = 100;
    let allocs = steady_state_allocs(BROWSER_GET_MIXED_CASE, hello, 50, n);
    let per_request = allocs as f64 / n as f64;
    println!(
        "browser GET (7 well-known + 6 mixed-case unknown headers): {allocs} over {n} ({per_request:.2}/request)"
    );
    assert_eq!(
        allocs,
        BUDGET_PER_MIXED_CASE_HEADER_GET * n as u64,
        "mixed-case unknown headers must cost exactly one allocation each per request \
         (the lowercasing-copy branch in src/parse.rs): {per_request:.2} per request, \
         budget is {BUDGET_PER_MIXED_CASE_HEADER_GET}"
    );
}

#[test]
fn fixed_body_post_stays_within_budget() {
    let n = 100;
    let allocs = steady_state_allocs(FIXED_BODY_POST, drain, 50, n);
    let per_request = allocs as f64 / n as f64;
    println!("Content-Length POST: {allocs} over {n} ({per_request:.2}/request)");
    assert!(
        allocs <= BUDGET_PER_GET * n as u64,
        "fixed-body POST budget exceeded: {per_request:.2} per request"
    );
}

#[test]
fn chunked_post_stays_within_budget() {
    let n = 100;
    let allocs = steady_state_allocs(CHUNKED_POST, drain, 50, n);
    let per_request = allocs as f64 / n as f64;
    println!("chunked POST: {allocs} over {n} ({per_request:.2}/request)");
    assert!(
        allocs <= BUDGET_PER_GET * n as u64,
        "chunked POST budget exceeded: {per_request:.2} per request"
    );
}

/// The property that matters most: cost per request must not grow with the
/// number of requests served.
///
/// A budget test alone would pass even if the connection leaked an allocation
/// per request into a growing structure; comparing two window sizes catches
/// that, because a leak makes the later window more expensive than the earlier
/// one.
#[test]
fn per_request_cost_does_not_grow_with_connection_age() {
    let early = steady_state_allocs(KEEPALIVE_GET, hello, 20, 50) as f64 / 50.0;
    let late = steady_state_allocs(KEEPALIVE_GET, hello, 500, 50) as f64 / 50.0;
    println!("early: {early:.2}/request, after 500 requests: {late:.2}/request");
    assert!(
        late <= early,
        "per-request cost grew with connection age: {early:.2} -> {late:.2}"
    );
}
