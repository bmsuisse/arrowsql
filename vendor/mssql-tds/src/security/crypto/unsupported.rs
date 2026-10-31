// RUST-PORT: Always Encrypted crypto backend stub for builds without the
// `native-tls-backend` (OpenSSL) feature. Every primitive fails cleanly.

use crate::core::TdsResult;
use crate::error::Error;

fn unsupported<T>() -> TdsResult<T> {
    Err(Error::ColumnEncryptionError(
        "Always Encrypted is not supported in this build (compiled without OpenSSL)".to_string(),
    ))
}

pub(crate) fn fill_random(_buf: &mut [u8]) -> TdsResult<()> {
    unsupported()
}

pub(crate) fn hmac_sha256(_key: &[u8], _data: &[u8]) -> TdsResult<[u8; 32]> {
    unsupported()
}

pub(crate) fn aes_256_cbc_encrypt(
    _key: &[u8; 32],
    _iv: &[u8; 16],
    _plaintext: &[u8],
) -> TdsResult<Vec<u8>> {
    unsupported()
}

pub(crate) fn aes_256_cbc_decrypt(
    _key: &[u8; 32],
    _iv: &[u8; 16],
    _ciphertext: &[u8],
) -> TdsResult<Vec<u8>> {
    unsupported()
}

pub(crate) struct RsaKey;

#[allow(dead_code)]
impl RsaKey {
    pub(crate) fn from_pem(_pem: &[u8]) -> TdsResult<Self> {
        unsupported()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn generate(_bits: u32) -> TdsResult<Self> {
        unsupported()
    }

    #[cfg(test)]
    pub(crate) fn to_pkcs8_pem(&self) -> TdsResult<Vec<u8>> {
        unsupported()
    }

    pub(crate) fn oaep_sha1_encrypt(&self, _plaintext: &[u8]) -> TdsResult<Vec<u8>> {
        unsupported()
    }

    pub(crate) fn oaep_sha1_decrypt(&self, _ciphertext: &[u8]) -> TdsResult<Vec<u8>> {
        unsupported()
    }

    pub(crate) fn pkcs1_sha256_sign(&self, _data: &[u8]) -> TdsResult<Vec<u8>> {
        unsupported()
    }

    pub(crate) fn pkcs1_sha256_verify(&self, _data: &[u8], _signature: &[u8]) -> TdsResult<bool> {
        unsupported()
    }
}
