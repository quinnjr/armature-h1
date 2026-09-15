//! TLS dispatch: ALPN steering between HTTP/1.1 and the HTTP/2 fallback.
//!
//! Split out from `dispatch.rs` because `required-features` applies to a whole
//! test target: with these tests in that file, the plaintext h2c cases — which
//! touch no TLS at all — never ran under the default feature set, which is what
//! CI builds.

use armature_h1::{Config, H2Fallback, Limits, Request, Response, Server, Transport};
use bytes::Bytes;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const GET: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";

/// Records what the fallback received, so a test can assert on it from outside.
#[derive(Clone, Default)]
struct Recorder {
    seen: Arc<Mutex<Vec<Vec<u8>>>>,
    /// The peer address the fallback was handed. An ALPN-negotiated `h2`
    /// connection is served entirely by the fallback, so this is its only
    /// chance to learn which client it is serving.
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

struct Cert {
    der: Vec<u8>,
    key: Vec<u8>,
}

fn self_signed() -> Cert {
    let c = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert");
    Cert {
        der: c.cert.der().to_vec(),
        key: c.signing_key.serialize_der(),
    }
}

fn client_config(cert_der: &[u8], alpn: &[&[u8]]) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(cert_der.to_vec()))
        .expect("add root");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(cfg)
}

#[test]
fn tls_request_round_trips_over_alpn_http11() {
    let cert = self_signed();
    let server_tls = armature_h1::tls::TlsConfig::new(vec![cert.der.clone()], cert.key.clone())
        .server_config()
        .expect("server config");
    let client_tls = client_config(&cert.der, &[b"http/1.1"]);
    let rec = Recorder::default();
    let seen = rec.seen.clone();

    let reply = with_server(test_config().with_tls(server_tls), rec, move |addr| {
        Box::pin(async move {
            let connector = tokio_rustls::TlsConnector::from(client_tls);
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
            let mut s = connector.connect(name, tcp).await.expect("handshake");
            s.write_all(GET).await.unwrap();
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut out)).await;
            String::from_utf8_lossy(&out).into_owned()
        })
    });

    assert!(reply.starts_with("HTTP/1.1 200 OK"), "{reply}");
    assert!(reply.ends_with("hi"), "{reply}");
    assert!(
        seen.lock().unwrap().is_empty(),
        "http/1.1 must not fall back"
    );
}

#[test]
fn tls_alpn_h2_reaches_the_fallback() {
    let cert = self_signed();
    let server_tls = armature_h1::tls::TlsConfig::new(vec![cert.der.clone()], cert.key.clone())
        .with_h2()
        .server_config()
        .expect("server config");
    let client_tls = client_config(&cert.der, &[b"h2"]);
    let rec = Recorder::default();
    let seen = rec.seen.clone();

    let reply = with_server(test_config().with_tls(server_tls), rec, move |addr| {
        Box::pin(async move {
            let connector = tokio_rustls::TlsConnector::from(client_tls);
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
            let mut s = connector.connect(name, tcp).await.expect("handshake");
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut out)).await;
            out
        })
    });

    assert_eq!(
        reply, b"FALLBACK",
        "a negotiated h2 must reach the fallback"
    );
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(
        seen[0].is_empty(),
        "nothing is read past the handshake, so there is no buffered app data"
    );
}

/// A peer that starts a handshake and then says nothing must be hung up on.
///
/// Until the handshake completes there is no `TlsStream`, so none of the
/// connection's own deadlines — `header_timeout` on the request head,
/// `idle_timeout` between requests — is armed against anything: they all live
/// on the far side of `acceptor.accept`. The handshake is therefore deadlined
/// by `header_timeout` explicitly, and that `timeout` wrapper is a single line
/// with nothing else depending on it. Remove it and a peer that connects, sends
/// four bytes and waits holds a task and a file descriptor for as long as it
/// likes — and under `SO_REUSEPORT` it can aim every such socket at one worker,
/// which is a whole core's worth of connections held by a client that has sent
/// no request at all.
///
/// `test_config` already sets a 300 ms `header_timeout`, so the bound below is
/// generous by nearly an order of magnitude: it fails on "never", not on "slow".
#[test]
fn a_stalled_tls_handshake_is_dropped_rather_than_held() {
    let cert = self_signed();
    let server_tls = armature_h1::tls::TlsConfig::new(vec![cert.der.clone()], cert.key.clone())
        .server_config()
        .expect("server config");
    let rec = Recorder::default();
    let seen = rec.seen.clone();

    let (hung_up, elapsed) = with_server(test_config().with_tls(server_tls), rec, move |addr| {
        Box::pin(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
            // A TLS record header announcing a 512-byte handshake message,
            // followed by nine bytes of one. Well-formed as far as it goes and
            // deliberately incomplete: rustls cannot decide anything until the
            // rest arrives, which is the point — a malformed hello would be
            // rejected on its contents and would prove nothing about the
            // deadline.
            s.write_all(&[
                0x16, 0x03, 0x01, 0x02, 0x00, // record: handshake, TLS 1.0, 512 bytes
                0x01, 0x00, 0x01, 0xfc, // ClientHello, 508 bytes to follow
                0x03, 0x03, 0x00, 0x00, 0x00, // …of which these are five
            ])
            .await
            .expect("write a truncated ClientHello");
            s.flush().await.expect("flush");

            // …and then nothing. Never another byte, never a close.
            let started = std::time::Instant::now();
            let mut out = Vec::new();
            let hung_up = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut out))
                .await
                .is_ok();
            (hung_up, started.elapsed())
        })
    });

    assert!(
        hung_up,
        "the server still had the connection 2s after a handshake that stalled \
         on its first record, with a 300ms handshake deadline configured — so \
         the deadline is not being applied and a peer can hold a worker's fd \
         and task indefinitely by connecting and going quiet"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "the close must come from the deadline, not from the test's own \
         patience running out: {elapsed:?}"
    );
    assert!(
        seen.lock().expect("lock").is_empty(),
        "a connection that never finished its handshake negotiated no ALPN, so \
         it must not be handed to the h2 fallback"
    );
}

/// A client offering both must get `h2`, since the fallback exists to serve it.
#[test]
fn tls_alpn_prefers_h2_when_the_client_offers_both() {
    let cert = self_signed();
    let server_tls = armature_h1::tls::TlsConfig::new(vec![cert.der.clone()], cert.key.clone())
        .with_h2()
        .server_config()
        .expect("server config");
    let client_tls = client_config(&cert.der, &[b"h2", b"http/1.1"]);
    let rec = Recorder::default();
    let seen = rec.seen.clone();

    with_server(test_config().with_tls(server_tls), rec, move |addr| {
        Box::pin(async move {
            let connector = tokio_rustls::TlsConnector::from(client_tls);
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
            let mut s = connector.connect(name, tcp).await.expect("handshake");
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut out)).await;
        })
    });

    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "offering h2 and http/1.1 must negotiate h2"
    );
}
