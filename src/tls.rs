use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::rustls::ServerConfig;
use rustls_pki_types::CertificateDer;

pub fn load_tls_config(
    cert_path: &str,
    key_path: &str,
) -> Result<Arc<ServerConfig>, Box<dyn std::error::Error + Send + Sync>> {
    let cert_file = File::open(Path::new(cert_path))?;
    let mut cert_reader = BufReader::new(cert_file);
    
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            "Certificate file contains no certificates",
        )));
    }

    let key_file = File::open(Path::new(key_path))?;
    let mut key_reader = BufReader::new(key_file);
    
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| io::Error::new(
            io::ErrorKind::NotFound,
            "No private key found in key file",
        ))?;

    // We can handle both PKCS8, RSA and EC keys as PrivateKeyDer
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    config.alpn_protocols = vec![
        b"h3".to_vec(),
        b"h2".to_vec(),
        b"http/1.1".to_vec(),
    ];

    Ok(Arc::new(config))
}

pub fn load_acme_config(
    domain: &str,
    email: &str,
) -> Result<Arc<ServerConfig>, Box<dyn std::error::Error + Send + Sync>> {
    use rustls_acme::{AcmeConfig, caches::DirCache};
    use futures::StreamExt;
    use tracing::{info, error};

    let mut state = AcmeConfig::new(vec![domain.to_string()])
        .contact(vec![format!("mailto:{}", email)])
        .cache(DirCache::new("./letsencrypt"))
        .state();
        
    let config = state.challenge_rustls_config();
    
    // Spawn the background ACME task to handle challenge handshakes & certificate renewals
    tokio::spawn(async move {
        loop {
            match state.next().await {
                Some(Ok(event)) => {
                    info!("ACME Certificate Provisioning Event: {:?}", event);
                }
                Some(Err(err)) => {
                    error!("ACME Certificate Provisioning Error: {:?}", err);
                }
                None => break,
            }
        }
    });
    
    Ok(config)
}
