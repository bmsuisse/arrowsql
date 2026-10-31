// RUST-PORT: rustls-backed TlsEngine (tokio-rustls + `ring`), replaces
// native-tls/OpenSSL as the default so the wheel needs no libssl/libcrypto
// (and therefore no libzstd, which libcrypto pulls in).
//
// Semantics mirror native_tls_engine:
//   accept_invalid_certs      -> no chain/expiry/name check (TrustServerCertificate,
//                                LoginOnly encryption, ServerCertificate pinning)
//   accept_invalid_hostnames  -> chain validated, name mismatch tolerated
//   use_alpn                  -> "tds/8.0" (Encrypt=strict / TDS 8)
// The TDS-in-TLS prelogin wrapping lives in the base stream
// (`tls_handshake_starting/completed`), so it is engine independent.
// Limits: TLS 1.2 / 1.3 only (no TLS 1.0/1.1 servers), ECDHE suites only.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use async_trait::async_trait;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms, ring as ring_provider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::client::TlsStream;
use tracing::{debug, error, info};

use super::{TlsConnectParams, TlsEngine, TlsValidationConfig};
use crate::connection::transport::certificate_validator;
use crate::connection::transport::network_transport::Stream;
use crate::core::{TDS_8_ALPN_PROTOCOL, TdsResult};

/// Skips all certificate checks but still verifies handshake signatures.
#[derive(Debug)]
struct AcceptAnyCert(WebPkiSupportedAlgorithms);

impl AcceptAnyCert {
    /// SQL Server's auto-generated self-signed certificate is X.509 v1, which
    /// webpki refuses to parse. The certificate itself is not trusted here
    /// (TrustServerCertificate), so extract the public key with x509-parser and
    /// verify the handshake signature directly with the provider's algorithms.
    fn verify_lenient(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
        orig: rustls::Error,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        use x509_parser::prelude::*;
        let Ok((_, x509)) = X509Certificate::from_der(cert.as_ref()) else {
            return Err(orig);
        };
        let key = x509.public_key().subject_public_key.data.as_ref();
        for (scheme, algs) in self.0.mapping {
            if *scheme == dss.scheme {
                for alg in *algs {
                    if alg.verify_signature(key, message, dss.signature()).is_ok() {
                        return Ok(HandshakeSignatureValid::assertion());
                    }
                }
            }
        }
        Err(orig)
    }
}

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
            .or_else(|e| self.verify_lenient(message, cert, dss, e))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
            .or_else(|e| self.verify_lenient(message, cert, dss, e))
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// Validates the chain normally but tolerates a host name mismatch.
#[derive(Debug)]
struct IgnoreHostname(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for IgnoreHostname {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName))
            | Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

static PROVIDER: LazyLock<Arc<CryptoProvider>> =
    LazyLock::new(|| Arc::new(ring_provider::default_provider()));

/// System trust store, loaded once (it is the expensive part of a connector).
static ROOTS: LazyLock<Arc<RootCertStore>> = LazyLock::new(|| {
    let mut store = RootCertStore::empty();
    let loaded = rustls_native_certs::load_native_certs();
    for err in &loaded.errors {
        debug!("native cert load issue: {err}");
    }
    let (added, ignored) = store.add_parsable_certificates(loaded.certs);
    debug!("system trust store: {added} roots, {ignored} ignored");
    Arc::new(store)
});

static CONFIG_CACHE: LazyLock<Mutex<HashMap<TlsValidationConfig, Arc<ClientConfig>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn build_config(validation: &TlsValidationConfig) -> TdsResult<Arc<ClientConfig>> {
    let tls_err = |e: rustls::Error| crate::error::Error::TlsError(e.to_string());
    let builder = ClientConfig::builder_with_provider(PROVIDER.clone())
        .with_safe_default_protocol_versions()
        .map_err(tls_err)?;
    let mut config = if validation.accept_invalid_certs {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(
                PROVIDER.signature_verification_algorithms,
            )))
            .with_no_client_auth()
    } else {
        let webpki = WebPkiServerVerifier::builder_with_provider(ROOTS.clone(), PROVIDER.clone())
            .build()
            .map_err(|e| crate::error::Error::TlsError(e.to_string()))?;
        if validation.accept_invalid_hostnames {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(IgnoreHostname(webpki)))
                .with_no_client_auth()
        } else {
            builder
                .dangerous()
                .with_custom_certificate_verifier(webpki)
                .with_no_client_auth()
        }
    };
    if validation.use_alpn {
        config.alpn_protocols = vec![TDS_8_ALPN_PROTOCOL.as_bytes().to_vec()];
    }
    Ok(Arc::new(config))
}

fn get_or_build_config(validation: &TlsValidationConfig) -> TdsResult<Arc<ClientConfig>> {
    let mut cache = CONFIG_CACHE.lock().map_err(|_| {
        crate::error::Error::ImplementationError("TLS config cache lock poisoned".to_string())
    })?;
    if let Some(c) = cache.get(validation) {
        return Ok(c.clone());
    }
    let c = build_config(validation)?;
    cache.insert(validation.clone(), c.clone());
    Ok(c)
}

fn server_name(host: &str, lenient: bool) -> TdsResult<ServerName<'static>> {
    match ServerName::try_from(host.to_string()) {
        Ok(n) => Ok(n),
        // Names that are not valid DNS labels (underscores, ...) cannot be
        // checked anyway when name/cert checks are off; use a placeholder.
        Err(_) if lenient => Ok(ServerName::try_from("localhost".to_string()).expect("valid")),
        Err(e) => Err(crate::error::Error::TlsError(format!(
            "invalid host name '{host}' for certificate validation: {e}"
        ))),
    }
}

#[derive(Debug)]
pub(crate) struct RustlsEngine;

pub(crate) static RUSTLS_ENGINE: RustlsEngine = RustlsEngine;

#[async_trait]
impl TlsEngine for RustlsEngine {
    async fn connect(
        &self,
        mut base_stream: Box<dyn Stream>,
        params: TlsConnectParams<'_>,
    ) -> TdsResult<Box<dyn Stream>> {
        base_stream.tls_handshake_starting();

        let lenient =
            params.validation.accept_invalid_certs || params.validation.accept_invalid_hostnames;
        let config = get_or_build_config(params.validation)?;
        let name = server_name(params.host_name, lenient)?;

        info!(
            "Starting TLS handshake to {} using host {}",
            params.server_host_name, params.host_name
        );
        let connector = tokio_rustls::TlsConnector::from(config);
        match connector.connect(name, base_stream).await {
            Ok(mut stream) => {
                if params.validation.use_alpn {
                    match stream.get_ref().1.alpn_protocol() {
                        Some(p) => debug!("Server negotiated ALPN: {}", String::from_utf8_lossy(p)),
                        None => debug!("Server did not negotiate an ALPN protocol"),
                    }
                }

                if let Some(cert_path) = params.server_certificate_path {
                    info!("Validating server certificate using: {cert_path:?}");
                    let der = stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(|c| c.first())
                        .ok_or(crate::error::Error::NoServerCertificate)?
                        .as_ref()
                        .to_vec();
                    certificate_validator::validate_server_certificate(cert_path, &der)?;
                    info!("Server certificate validation successful");
                }

                stream.get_mut().0.tls_handshake_completed();
                Ok(Box::new(RustlsStream(stream)))
            }
            Err(e) => {
                error!(
                    "TLS handshake FAILED: host_name={}, server_host_name={}, error={:?}, os={}",
                    params.host_name,
                    params.server_host_name,
                    e,
                    std::env::consts::OS,
                );
                Err(crate::error::Error::TlsHandshakeError {
                    source: Box::new(e),
                    expected_host: params.host_name.to_string(),
                    cert_sans:
                        "(unavailable - handshake failed before certificate could be retrieved)"
                            .to_string(),
                })
            }
        }
    }
}

/// Adapts tokio-rustls to the TDS layer's expectations:
///
/// * The TDS layer never flushes (native-tls writes through), but tokio-rustls
///   keeps encrypted output queued when the socket is momentarily not writable
///   (or tokio's cooperative budget is spent). Pending output is therefore
///   completed before blocking on a read.
/// * SQL Server mishandles a TDS packet that is cut into TLS records at
///   arbitrary offsets. With output queued, tokio-rustls accepts only part of a
///   write (up to its 64 KiB send buffer) and rustls then emits that fragment as
///   a record of its own, splitting the TDS packet; the server drops the
///   connection after about 128 packets (~2 MB). Draining the queue before each
///   write guarantees every write is accepted whole, so records stay aligned with
///   the TDS packets exactly as with OpenSSL.
pub(crate) struct RustlsStream(TlsStream<Box<dyn Stream>>);

impl AsyncRead for RustlsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.0.get_ref().1.wants_write() {
            match Pin::new(&mut self.0).poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for RustlsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.0.get_ref().1.wants_write() {
            match Pin::new(&mut self.0).poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl Stream for RustlsStream {
    fn tls_handshake_starting(&mut self) {
        self.0.get_mut().0.tls_handshake_starting();
    }

    fn tls_handshake_completed(&mut self) {
        self.0.get_mut().0.tls_handshake_completed();
    }

    fn is_connection_dead(&self) -> bool {
        self.0.get_ref().0.is_connection_dead()
    }
}
