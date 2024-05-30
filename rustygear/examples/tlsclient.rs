//use tokio::prelude::*;

#[cfg(feature = "tls")]
use std::{fmt::Debug, sync::Arc};

#[cfg(feature = "tls")]
use rustygear::client::Client;
#[cfg(feature = "tls")]
use tokio_rustls::rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    ClientConfig, SignatureScheme,
};
#[derive(Debug)]
struct DebugVerifier {}

#[cfg(feature = "tls")]
impl ServerCertVerifier for DebugVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error>
    {
        println!("end_entity: {:?}", end_entity);
        println!("intermediates: {:?}", intermediates);
        println!("server_name: {:?}", server_name);
        println!("ocsp_response: {:?}", ocsp_response);
        println!("now: {:?}", now);
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        println!("message: {}", String::from_utf8_lossy(message));
        println!("cert: {:?}", cert);
        println!("dss: {:?}", dss);
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        println!("TLS1.3");
        println!("message: {}", String::from_utf8_lossy(message));
        println!("cert: {:?}", cert);
        println!("dss: {:?}", dss);
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

#[cfg(not(feature = "tls"))]
fn main() {
    println!("TLS not enabled.")
}

#[tokio::main]
#[cfg(feature = "tls")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let server = "localhost:4730";
    // This commented out approach is how one might configure the client using a local CA file.
    /*let mut root_store = RootCertStore::empty();
    let mut root_der: Vec<u8> = Vec::new();
    let _ = File::open("./ca.der")
        .expect("Couldn't open ca.der")
        .read_to_end(&mut root_der);
    match root_store.add(root_der.clone().into()) {
        Ok(_) => (),
        Err(e) => panic!(
            "Cert did not parse {:?} {}",
            String::from_utf8_lossy(&root_der),
            e
        ),
    };
    */
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(DebugVerifier {}))
        .with_no_client_auth(); /*
                                .with_root_certificates(root_store)
                                .with_no_client_auth();*/
    let mut client = Client::new()
        .add_server(server)
        .set_tls_config(config)
        .connect()
        .await?;
    println!("Connected!");
    println!("Echo: {:?}", client.echo(b"blah").await);
    Ok(())
}
