//! Protocol dispatch over plaintext: the h2c prior-knowledge preface.
//!
//! These run against a live `Server` on a real socket, because the thing under
//! test is the wiring — that a connection this crate will not serve actually
//! reaches the fallback with its bytes intact, rather than being fed to the
//! HTTP/1 parser.
//!
//! Deliberately not gated on the `tls` feature. `required-features` applies to a
//! whole test target, so keeping the ALPN tests here (they live in
//! `tls_dispatch.rs`) meant none of this ran under `default = []` — which is the
//! feature set CI builds.

use armature_h1::{
    Config, H2Fallback, HeaderId, Limits, Request, Response, ResponseBody, Server, Transport,
    UpgradeConsumer, Upgraded,
};
use bytes::Bytes;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const H2C_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const GET: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";

/// Records what the fallback received, so a test can assert on it from outside.
#[derive(Clone, Default)]
struct Recorder {
    seen: Arc<Mutex<Vec<Vec<u8>>>>,
    /// The peer address the fallback was handed, so a test can assert an
    /// HTTP/2 connection is not served with an unknown client.
    peers: Arc<Mutex<Vec<Option<SocketAddr>>>>,
}

impl H2Fallback for Recorder {
    fn handle(
        &self,
        io: Box<dyn Transport>,
        buffered: Bytes,
        peer: Option<SocketAddr>,
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        let seen = self.seen.clone();
        self.peers.lock().expect("lock").push(peer);
        Box::pin(async move {
            seen.lock().expect("lock").push(buffered.to_vec());
            // Reply with something recognizable so the client can tell the
            // fallback ran, rather than inferring it from silence.
            let mut io = io;
            let _ = io.write_all(b"FALLBACK").await;
            let _ = io.flush().await;
        })
    }
}

fn test_config() -> Config {
    let limits = Limits {
        idle_timeout: Duration::from_millis(300),
        header_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    Config::new(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
        .workers(1)
        .limits(limits)
        .pin_cores(false)
}

async fn hello(_req: Request) -> Response {
    Response::text("hi")
}

/// Run `body` against a live server built from `cfg`, with `fallback`.
fn with_server<T>(
    cfg: Config,
    fallback: Recorder,
    body: impl FnOnce(SocketAddr) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + 'static,
) -> T
where
    T: Send + 'static,
{
    let server = Server::bind(cfg).expect("bind");
    let addr = server.local_addr();
    let handle = server.handle();

    let client = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(body(addr));
        handle.shutdown();
        out
    });

    server
        .serve_with_fallback(|| hello, move || fallback.clone())
        .expect("serve");
    client.join().expect("client thread")
}

#[test]
fn h2c_preface_reaches_the_fallback_with_its_bytes() {
    let rec = Recorder::default();
    let seen = rec.seen.clone();

    let reply = with_server(test_config().detect_h2c(true), rec, |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(H2C_PREFACE).await.unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out)).await;
            out
        })
    });

    assert_eq!(reply, b"FALLBACK", "the fallback must have handled it");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "exactly one connection reached the fallback");
    assert_eq!(
        seen[0], H2C_PREFACE,
        "the preface is part of the HTTP/2 stream and cannot be re-read from the \
         socket, so it must arrive in `buffered` intact"
    );
}

#[test]
fn http1_request_is_served_normally_with_h2c_detection_on() {
    let rec = Recorder::default();
    let seen = rec.seen.clone();

    let reply = with_server(test_config().detect_h2c(true), rec, |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(GET).await.unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out)).await;
            String::from_utf8_lossy(&out).into_owned()
        })
    });

    assert!(reply.starts_with("HTTP/1.1 200 OK"), "{reply}");
    assert!(reply.ends_with("hi"), "{reply}");
    assert!(
        seen.lock().unwrap().is_empty(),
        "an HTTP/1 request must not reach the HTTP/2 fallback"
    );
}

/// The bytes consumed while classifying are part of the request and must be
/// handed to the HTTP/1 parser, not dropped.
#[test]
fn peeked_bytes_are_not_lost_from_the_http1_request() {
    let rec = Recorder::default();
    let reply = with_server(test_config().detect_h2c(true), rec, |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            // Dribble the request one byte at a time, forcing the classifier to
            // consume several reads before deciding.
            for b in GET {
                s.write_all(&[*b]).await.unwrap();
                s.flush().await.unwrap();
            }
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut out)).await;
            String::from_utf8_lossy(&out).into_owned()
        })
    });
    assert!(reply.starts_with("HTTP/1.1 200 OK"), "{reply}");
}

#[test]
#[cfg_attr(
    feature = "hyper-backend",
    ignore = "see BACKENDS.md: status for an unparseable h2c preface without \
              detection (hyper closes the connection with no response instead \
              of writing a 4xx/5xx)"
)]
fn h2c_without_detection_is_parsed_as_http1_and_rejected() {
    let rec = Recorder::default();
    let seen = rec.seen.clone();

    // With detection off, `PRI * HTTP/2.0` is just an unparseable HTTP/1 request
    // line. It must be rejected, never silently accepted.
    let reply = with_server(test_config(), rec, |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(H2C_PREFACE).await.unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out)).await;
            String::from_utf8_lossy(&out).into_owned()
        })
    });

    assert!(
        reply.starts_with("HTTP/1.1 4") || reply.starts_with("HTTP/1.1 5"),
        "must not be served as a valid request: {reply}"
    );
    assert!(seen.lock().unwrap().is_empty());
}

/// Positive twin of `h2c_without_detection_is_parsed_as_http1_and_rejected`
/// for the hyper backend: BACKENDS.md documents hyper's h1 parser abandoning
/// the connection on this input before any error-response hook fires, so it
/// closes with zero response bytes rather than writing a 4xx/5xx. If a
/// future hyper starts producing a status here, that is exactly the signal
/// the ignored original test is watching for.
#[test]
#[cfg(feature = "hyper-backend")]
fn hyper_h2c_without_detection_closes_with_no_response() {
    let rec = Recorder::default();
    let seen = rec.seen.clone();

    let reply = with_server(test_config(), rec, |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(H2C_PREFACE).await.unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out)).await;
            String::from_utf8_lossy(&out).into_owned()
        })
    });

    assert!(
        reply.is_empty(),
        "hyper abandons the connection before writing any response: {reply}"
    );
    assert!(seen.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Peer address, and the upgrade-consumer hook.
// ---------------------------------------------------------------------------

/// Like [`with_server`], but with an upgrade consumer plugged in as well.
fn with_server_upgrades<T, C>(
    cfg: Config,
    fallback: Recorder,
    upgrades: C,
    body: impl FnOnce(SocketAddr) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + 'static,
) -> T
where
    T: Send + 'static,
    C: UpgradeConsumer + Clone + Send + 'static,
{
    let server = Server::bind(cfg).expect("bind");
    let addr = server.local_addr();
    let handle = server.handle();

    let client = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(body(addr));
        handle.shutdown();
        out
    });

    server
        .serve_with(
            || echo_peer,
            move || fallback.clone(),
            move || upgrades.clone(),
        )
        .expect("serve");
    client.join().expect("client thread")
}

/// Answers with the peer address the connection reported, so a test can assert
/// on it from the client side without reaching into server internals.
async fn echo_peer(req: Request) -> Response {
    // An upgrade request gets a 101 so the upgrade-consumer path runs; anything
    // else gets the peer address in the body.
    if req.head.connection_has_token("upgrade") {
        // The request body holds a handle on the transport, and a live handle
        // forfeits the handoff — so it must go before the 101 does.
        drop(req.body);
        return Response::new(101)
            .header(HeaderId::Connection, Bytes::from_static(b"upgrade"))
            .header(HeaderId::Upgrade, Bytes::from_static(b"raw"));
    }
    let peer = req
        .peer
        .map_or_else(|| "none".to_string(), |p| p.to_string());
    Response::new(200).with_body(ResponseBody::Full(Bytes::from(peer)))
}

/// Takes the upgraded transport and speaks the "protocol" the test expects.
#[derive(Clone, Default)]
struct RawUpgrade {
    seen: Arc<Mutex<Vec<Vec<u8>>>>,
    /// The peer each handed-off connection reported. An upgrade consumer owns
    /// the connection for its whole lifetime, so this is the only chance it has
    /// to learn the address.
    peers: Arc<Mutex<Vec<Option<SocketAddr>>>>,
}

impl UpgradeConsumer for RawUpgrade {
    fn handle(&self, upgraded: Upgraded) -> Pin<Box<dyn Future<Output = ()>>> {
        let seen = self.seen.clone();
        let peers = self.peers.clone();
        Box::pin(async move {
            peers.lock().expect("lock").push(upgraded.peer);
            seen.lock().expect("lock").push(upgraded.buffered.to_vec());
            let mut io = upgraded.io;
            let _ = io.write_all(b"UPGRADED").await;
            let _ = io.flush().await;
        })
    }
}

#[test]
fn the_served_request_carries_the_connecting_peer_address() {
    let rec = Recorder::default();

    let (reply, local) = with_server_upgrades(test_config(), rec, RawUpgrade::default(), |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            // The client's own local address is, from the server's side, the
            // peer address — so the test knows the exact value to expect
            // rather than merely asserting "something non-empty".
            let local = s.local_addr().unwrap();
            s.write_all(GET).await.unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out)).await;
            (String::from_utf8_lossy(&out).into_owned(), local)
        })
    });

    assert!(
        reply.ends_with(&local.to_string()),
        "the handler must see the connecting socket's address, got: {reply}"
    );
}

#[test]
fn an_upgraded_connection_reaches_the_upgrade_consumer_with_its_bytes() {
    let rec = Recorder::default();
    let up = RawUpgrade::default();
    let seen = up.seen.clone();

    let peers = up.peers.clone();

    let (reply, local) = with_server_upgrades(test_config(), rec, up, |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            // The client's own local address is, from the server's side, the
            // peer address, so the exact expected value is known here.
            let local = s.local_addr().unwrap();
            // The first post-upgrade frame is pipelined into the same write, so
            // it lands in the connection's read buffer before the handoff —
            // which is exactly the data `Upgraded::buffered` exists to carry.
            s.write_all(
                b"GET / HTTP/1.1\r\nHost: a\r\nConnection: upgrade\r\nUpgrade: raw\r\n\r\nFIRSTFRAME",
            )
            .await
            .unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out)).await;
            (String::from_utf8_lossy(&out).into_owned(), local)
        })
    });

    assert!(
        reply.starts_with("HTTP/1.1 101"),
        "the 101 must go out before the handoff: {reply}"
    );
    assert!(
        reply.ends_with("UPGRADED"),
        "the consumer owns the transport after the 101 and its bytes must reach \
         the client: {reply}"
    );
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "exactly one connection was handed off");
    assert_eq!(
        seen[0], b"FIRSTFRAME",
        "bytes read past the upgrade request's head cannot be re-read from the \
         socket, so they must arrive in `buffered` intact"
    );
    let peers = peers.lock().unwrap();
    assert_eq!(
        peers[0],
        Some(local),
        "the address is read off the connection just before `into_parts` \
         consumes it, and the consumer holds the socket for the whole session \
         with no `Request` left to ask; losing it here means every upgraded \
         connection is served with an unknown client"
    );
}

#[test]
fn without_an_upgrade_consumer_an_upgraded_connection_still_closes() {
    let server = Server::bind(test_config()).expect("bind");
    let addr = server.local_addr();
    let handle = server.handle();

    let client = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(
                b"GET / HTTP/1.1\r\nHost: a\r\nConnection: upgrade\r\nUpgrade: raw\r\n\r\n",
            )
            .await
            .unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out)).await;
            String::from_utf8_lossy(&out).into_owned()
        });
        handle.shutdown();
        out
    });

    // `serve` defaults to `CloseUpgrade`: the 101 goes out and the socket is
    // then closed rather than left open with nobody reading it.
    server.serve(|| echo_peer).expect("serve");
    let reply = client.join().expect("client thread");

    assert!(
        reply.starts_with("HTTP/1.1 101"),
        "the response is still written: {reply}"
    );
    assert!(
        reply.ends_with("\r\n\r\n"),
        "nothing follows the 101 when no consumer is plugged in: {reply}"
    );
}

#[test]
fn the_http2_fallback_is_told_which_peer_it_is_serving() {
    let rec = Recorder::default();
    let peers = rec.peers.clone();

    let local = with_server(test_config().detect_h2c(true), rec, |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            let local = s.local_addr().unwrap();
            s.write_all(H2C_PREFACE).await.unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out)).await;
            local
        })
    });

    let peers = peers.lock().unwrap();
    assert_eq!(
        peers.len(),
        1,
        "exactly one connection reached the fallback"
    );
    assert_eq!(
        peers[0],
        Some(local),
        "a fallback serves whole connections and has no other way to learn the \
         client address; without it every HTTP/2 request it serves is attributed \
         to whatever the caller put in a header"
    );
}

#[test]
fn an_upgrade_on_a_request_with_an_unread_body_closes_instead_of_handing_off() {
    let rec = Recorder::default();
    let up = RawUpgrade::default();
    let seen = up.seen.clone();

    // `echo_peer` answers 101 after dropping the body without reading it. The
    // five declared body bytes are therefore still on the wire, and handing
    // them to the consumer would present request-body bytes as the peer's first
    // post-upgrade frames — the smuggling shape aimed at the consumer rather
    // than at the parser.
    let reply = with_server_upgrades(test_config(), rec, up, |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(
                b"POST / HTTP/1.1\r\nHost: a\r\nConnection: upgrade\r\nUpgrade: raw\r\n\
                  Content-Length: 5\r\n\r\nHELLO",
            )
            .await
            .unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out)).await;
            String::from_utf8_lossy(&out).into_owned()
        })
    });

    assert!(
        reply.starts_with("HTTP/1.1 101"),
        "the response is still written: {reply}"
    );
    assert!(
        !reply.ends_with("UPGRADED"),
        "the handoff must be forfeited when the body was never read: {reply}"
    );
    assert!(
        seen.lock().unwrap().is_empty(),
        "no connection may reach the consumer with unread body bytes buffered"
    );
}
