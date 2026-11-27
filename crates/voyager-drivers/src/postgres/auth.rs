//! PostgreSQL authentication algorithms: Cleartext, MD5, and SASL SCRAM-SHA-256.

use base64::prelude::*;
use hmac::{Hmac, Mac};
use md5::{Digest as Md5Digest, Md5};
use sha2::Sha256;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{NetError, Result};

type HmacSha256 = Hmac<Sha256>;

/// Minimum permitted SCRAM-SHA-256 PBKDF2 iteration count (RFC 7677).
pub const MIN_SCRAM_ITERATIONS: u32 = 4096;
/// Maximum permitted SCRAM-SHA-256 PBKDF2 iteration count to prevent CPU DoS.
pub const MAX_SCRAM_ITERATIONS: u32 = 1_000_000;

static FALLBACK_NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generates the MD5 authentication response for PostgreSQL.
///
/// Formula: `"md5" + hex(md5(hex(md5(password + user)) + salt))`
pub fn compute_md5_password(user: &str, password: &str, salt: &[u8; 4]) -> Vec<u8> {
    // Step 1: inner = md5(password + user)
    let mut inner_hasher = Md5::new();
    inner_hasher.update(password.as_bytes());
    inner_hasher.update(user.as_bytes());
    let inner_hash = inner_hasher.finalize();
    let inner_hex = hex::encode(inner_hash);

    // Step 2: outer = md5(inner_hex + salt)
    let mut outer_hasher = Md5::new();
    outer_hasher.update(inner_hex.as_bytes());
    outer_hasher.update(salt);
    let outer_hash = outer_hasher.finalize();
    let outer_hex = hex::encode(outer_hash);

    let mut result = format!("md5{}", outer_hex).into_bytes();
    result.push(0); // Null-terminated string
    result
}

/// Helper function to compute HMAC-SHA256.
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}

/// Helper function to compute SHA-256 digest.
fn sha256_digest(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}

/// PBKDF2-HMAC-SHA256 single-block key derivation (RFC 5802 / RFC 2898).
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut salt_with_int = Vec::with_capacity(salt.len() + 4);
    salt_with_int.extend_from_slice(salt);
    salt_with_int.extend_from_slice(&[0, 0, 0, 1]); // Big-endian integer 1

    let mut u = hmac_sha256(password, &salt_with_int);
    let mut result = u;

    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for j in 0..32 {
            result[j] ^= u[j];
        }
    }

    result
}

/// Generates a cryptographically random 24-character client nonce for SCRAM.
pub fn try_generate_client_nonce() -> Result<String> {
    let mut bytes = [0u8; 18];
    getrandom::fill(&mut bytes).map_err(|e| {
        NetError::AuthenticationFailed(format!(
            "OS CSPRNG failed while generating SCRAM client nonce: {e}"
        ))
    })?;
    Ok(BASE64_STANDARD.encode(bytes))
}

/// Generates a random 24-character client nonce for SCRAM using the OS CSPRNG.
pub fn generate_client_nonce() -> String {
    let mut bytes = [0u8; 18];
    if getrandom::fill(&mut bytes).is_ok() {
        return BASE64_STANDARD.encode(bytes);
    }

    // Panic-free fallback only if the OS CSPRNG itself returns an error.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = FALLBACK_NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut seed = [0u8; 24];
    seed[..16].copy_from_slice(&nanos.to_le_bytes());
    seed[16..].copy_from_slice(&seq.to_le_bytes());
    let digest = sha256_digest(&seed);
    bytes.copy_from_slice(&digest[..18]);
    BASE64_STANDARD.encode(bytes)
}

/// State holder for multi-turn SCRAM-SHA-256 authentication.
#[derive(Debug, Clone)]
pub struct ScramClient {
    client_nonce: String,
    client_first_message_bare: String,
    server_signature: Option<[u8; 32]>,
}

impl ScramClient {
    /// Initializes a new SCRAM-SHA-256 client session using the OS CSPRNG, returning an error if CSPRNG fails.
    pub fn try_new() -> Result<Self> {
        let client_nonce = try_generate_client_nonce()?;
        let client_first_message_bare = format!("n=,r={}", client_nonce);
        Ok(Self {
            client_nonce,
            client_first_message_bare,
            server_signature: None,
        })
    }

    /// Initializes a new SCRAM-SHA-256 client session.
    pub fn new() -> Self {
        let client_nonce = generate_client_nonce();
        let client_first_message_bare = format!("n=,r={}", client_nonce);
        Self {
            client_nonce,
            client_first_message_bare,
            server_signature: None,
        }
    }

    /// Generates the initial client SASL message payload (`client-first-message`).
    pub fn client_first_message(&self) -> Vec<u8> {
        format!("n,,{}", self.client_first_message_bare).into_bytes()
    }

    /// Processes server-first-message challenge and generates `client-final-message`.
    pub fn process_challenge(&mut self, challenge: &[u8], password: &str) -> Result<Vec<u8>> {
        let server_first_message = std::str::from_utf8(challenge).map_err(|e| {
            NetError::AuthenticationFailed(format!(
                "Invalid UTF-8 in SCRAM server challenge: {}",
                e
            ))
        })?;

        // Parse challenge key-value pairs (r=<nonce>,s=<salt>,i=<iterations>)
        let mut server_nonce = None;
        let mut salt_b64 = None;
        let mut iterations = None;

        for part in server_first_message.split(',') {
            if let Some(rest) = part.strip_prefix("r=") {
                server_nonce = Some(rest);
            } else if let Some(rest) = part.strip_prefix("s=") {
                salt_b64 = Some(rest);
            } else if let Some(rest) = part.strip_prefix("i=") {
                iterations = rest.parse::<u32>().ok();
            }
        }

        let combined_nonce = server_nonce.ok_or_else(|| {
            NetError::AuthenticationFailed(
                "Missing 'r' nonce parameter in SCRAM challenge".to_string(),
            )
        })?;
        let salt_str = salt_b64.ok_or_else(|| {
            NetError::AuthenticationFailed(
                "Missing 's' salt parameter in SCRAM challenge".to_string(),
            )
        })?;
        let iters = iterations.ok_or_else(|| {
            NetError::AuthenticationFailed(
                "Missing 'i' iterations parameter in SCRAM challenge".to_string(),
            )
        })?;

        if !(MIN_SCRAM_ITERATIONS..=MAX_SCRAM_ITERATIONS).contains(&iters) {
            return Err(NetError::AuthenticationFailed(format!(
                "SCRAM iteration count {} outside permitted range {}..={}",
                iters, MIN_SCRAM_ITERATIONS, MAX_SCRAM_ITERATIONS
            )));
        }

        if !combined_nonce.starts_with(&self.client_nonce) {
            return Err(NetError::AuthenticationFailed(
                "Server nonce does not match client nonce in SCRAM exchange".to_string(),
            ));
        }

        let salt = BASE64_STANDARD.decode(salt_str).map_err(|e| {
            NetError::AuthenticationFailed(format!("Invalid base64 salt in SCRAM challenge: {}", e))
        })?;

        // Compute keys and proof according to RFC 5802 / RFC 7677
        let salted_password = pbkdf2_sha256(password.as_bytes(), &salt, iters);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256_digest(&client_key);

        let client_final_message_without_proof = format!("c=biws,r={}", combined_nonce);
        let auth_message = format!(
            "{},{},{}",
            self.client_first_message_bare,
            server_first_message,
            client_final_message_without_proof
        );

        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }

        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let server_sig = hmac_sha256(&server_key, auth_message.as_bytes());
        self.server_signature = Some(server_sig);

        let client_final = format!(
            "{},p={}",
            client_final_message_without_proof,
            BASE64_STANDARD.encode(client_proof)
        );

        Ok(client_final.into_bytes())
    }

    /// Verifies server-final-message (`v=<server_signature>`).
    pub fn verify_server_final(&self, final_data: &[u8]) -> Result<()> {
        let expected_sig = self.server_signature.ok_or_else(|| {
            NetError::AuthenticationFailed(
                "SCRAM state error: Server signature not calculated".to_string(),
            )
        })?;

        let final_str = std::str::from_utf8(final_data).map_err(|e| {
            NetError::AuthenticationFailed(format!(
                "Invalid UTF-8 in SCRAM server final message: {}",
                e
            ))
        })?;

        let mut server_sig_b64 = None;
        for part in final_str.split(',') {
            if let Some(rest) = part.strip_prefix("v=") {
                server_sig_b64 = Some(rest);
            }
        }

        let sig_str = server_sig_b64.ok_or_else(|| {
            NetError::AuthenticationFailed(
                "Missing 'v' server signature in SCRAM final message".to_string(),
            )
        })?;

        let sig_bytes = BASE64_STANDARD.decode(sig_str).map_err(|e| {
            NetError::AuthenticationFailed(format!(
                "Invalid base64 signature in SCRAM final: {}",
                e
            ))
        })?;

        if sig_bytes.len() != expected_sig.len() {
            return Err(NetError::AuthenticationFailed(
                "SCRAM server signature verification mismatch".to_string(),
            ));
        }

        let mut diff = 0u8;
        for (a, b) in sig_bytes.iter().zip(expected_sig.iter()) {
            diff |= a ^ b;
        }
        if diff != 0 {
            return Err(NetError::AuthenticationFailed(
                "SCRAM server signature verification mismatch".to_string(),
            ));
        }

        Ok(())
    }
}

impl Default for ScramClient {
    fn default() -> Self {
        Self::new()
    }
}
