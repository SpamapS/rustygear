#[macro_use]
extern crate log;
extern crate env_logger;

extern crate rustygear;
extern crate rustygeard;

use std::{
    fs::File,
    io::BufReader,
    process::{exit, ExitCode},
    sync::Arc,
};

use clap::{command, Arg, ArgAction};
use rustls_pemfile::{certs, private_key};
use rustygeard::server::GearmanServer;
use tokio_rustls::rustls::{
    server::{VerifierBuilderError, WebPkiClientVerifier},
    RootCertStore, ServerConfig,
};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() -> ExitCode {
    let matches = command!()
        .version(VERSION)
        .about("See http://gearman.org/protocol")
        .arg(
            Arg::new("listen")
                .short('L')
                .long("listen")
                .value_name("Address:port")
                .help("Server will listen on this address")
                .default_value("0.0.0.0:4730")
                .num_args(1),
        )
        .arg(
            Arg::new("tls")
                .long("tls")
                .help("Enable TLS")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("tls-key")
                .long("tls-key")
                .help("Path to PEM encoded TLS key file")
                .num_args(1)
                .requires("tls-key")
                .requires("tls-cert"),
        )
        .arg(
            Arg::new("tls-cert")
                .long("tls-cert")
                .help("Path to PEM encoded server certificate")
                .num_args(1)
                .requires("tls")
                .requires("tls-key"),
        )
        .arg(
            Arg::new("tls-ca")
                .long("tls-ca")
                .help("Path to CA for validating client certificates in TLS")
                .num_args(1)
                .requires("tls"),
        )
        .get_matches();

    let listen = matches
        .get_one::<String>("listen")
        .expect("default_value should ensure this");
    env_logger::init();

    info!("Binding to {}", listen);
    let address = listen.parse().unwrap();
    let tls = match matches.get_flag("tls") {
        false => None,
        true => {
            let builder = ServerConfig::builder();
            let builder = match matches.get_one::<String>("tls-ca") {
                None => {
                    warn!("Client certs will not be ignored.");
                    builder.with_no_client_auth()
                }
                Some(cafile) => match File::open(cafile) {
                    Ok(pemfile) => {
                        let mut roots = RootCertStore::empty();
                        for cert in certs(&mut BufReader::new(pemfile)) {
                            match cert {
                                Err(e) => {
                                    error!("{}", e);
                                    exit(-1);
                                }
                                Ok(cert) => {
                                    if let Err(e) = roots.add(cert) {
                                        error!(
                                            "There was an error parsing certs in {}: {}",
                                            cafile, e
                                        );
                                        return ExitCode::FAILURE;
                                    }
                                }
                            }
                        }
                        let verifier = match WebPkiClientVerifier::builder(Arc::new(roots)).build()
                        {
                            Ok(verifier) => verifier,
                            Err(e) => match e {
                                VerifierBuilderError::NoRootAnchors => {
                                    error!("No root anchors in tls-ca file?");
                                    return ExitCode::FAILURE;
                                }
                                VerifierBuilderError::InvalidCrl(_) => panic!("We don't use CRLs?"),
                                _ => panic!("Unexpected error in WebPKI"),
                            },
                        };
                        builder.with_client_cert_verifier(verifier)
                    }
                    Err(e) => {
                        error!("tls-ca file could not be opened: {}", e);
                        exit(e.raw_os_error().unwrap_or(-1))
                    }
                },
            };
            let keyfile = matches
                .get_one::<String>("tls-key")
                .expect("tls-key is required for --tls");
            let keyder = match File::open(keyfile) {
                Ok(pemfile) => match private_key(&mut BufReader::new(pemfile)) {
                    Ok(key) => match key {
                        Some(key) => key,
                        None => {
                            error!("tls-key file has no private keys in it");
                            return ExitCode::FAILURE;
                        }
                    },
                    Err(e) => {
                        error!("There was a problem reading from the tls-key file: {}", e);
                        return ExitCode::FAILURE;
                    }
                },
                Err(e) => {
                    error!("Could not open tls-key file: {} {}", keyfile, e);
                    return ExitCode::FAILURE;
                }
            };
            let certsfile = matches
                .get_one::<String>("tls-cert")
                .expect("tls-cert is required for --tls");
            let mut certs_vec = Vec::new();
            match File::open(certsfile) {
                Ok(certsfile) => {
                    for cert in certs(&mut BufReader::new(certsfile)) {
                        match cert {
                            Ok(cert) => certs_vec.push(cert),
                            Err(e) => {
                                error!(
                                    "There was a problem parsing one of the certs in the chain: {}",
                                    e
                                );
                                return ExitCode::FAILURE;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Could not open tls-cert file: {} {}", keyfile, e);
                    return ExitCode::FAILURE;
                }
            }
            Some(
                builder
                    .with_single_cert(certs_vec, keyder)
                    .expect("We already validated keyder"),
            )
        }
    };
    if let Err(e) = GearmanServer::run(address, tls) {
        eprintln!("Error: {}", e);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
