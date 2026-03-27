//! Utility functions for the wallet toolbox.
//!
//! This module provides security-related utilities for use in authentication
//! and cryptographic contexts throughout the crate.

/// Performs a constant-time comparison of two byte slices.
///
/// This function is designed to prevent timing side-channel attacks when
/// comparing security-sensitive values such as HMACs, MACs, or tokens.
///
/// Unlike the standard `==` operator on slices, which may return early on the
/// first differing byte (leaking information about how many leading bytes match),
/// this function always examines all bytes before returning.
///
/// Uses XOR accumulation to avoid branch-dependent timing differences.
/// The length check at the start is not constant-time, but this only reveals
/// whether the lengths differ, not the content. For HMAC comparisons the
/// lengths are always equal (both 32 bytes for SHA-256).
///
/// # When to use this function
///
/// Use `constant_time_eq` instead of `==` whenever comparing:
/// - HMAC values or MAC tags
/// - Authentication tokens or session identifiers
/// - Password hashes or derived key material
/// - Any byte sequence where a timing difference could leak information
///
/// Note: In this crate, HMAC verification (`verify_hmac`) and signature
/// verification (`verify_signature`) are delegated to `bsv-sdk`'s
/// `ProtoWallet`, which handles constant-time comparison internally.
/// This function is available for any additional authentication-level
/// byte comparisons that may be introduced in the future or in
/// downstream code.
///
/// # Returns
///
/// `true` if both slices have the same length and identical contents, `false` otherwise.
///
/// # Examples
///
/// ```rust,ignore
/// use bsv_wallet_toolbox_rs::utils::constant_time_eq;
///
/// assert!(constant_time_eq(b"hello", b"hello"));
/// assert!(!constant_time_eq(b"hello", b"world"));
/// assert!(!constant_time_eq(b"short", b"longer"));
/// ```
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    // XOR all corresponding bytes together. If any pair differs, at least
    // one bit in `acc` will be set. This loop always runs for all bytes
    // regardless of where a difference occurs, preventing timing leaks.
    let mut acc: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }

    acc == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equal_byte_arrays_return_true() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(constant_time_eq(&[1, 2, 3, 4], &[1, 2, 3, 4]));
        assert!(constant_time_eq(&[0u8; 32], &[0u8; 32]));
        assert!(constant_time_eq(&[0xff; 64], &[0xff; 64]));
    }

    #[test]
    fn test_unequal_byte_arrays_return_false() {
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(&[1, 2, 3, 4], &[1, 2, 3, 5]));
        assert!(!constant_time_eq(&[0u8; 32], &[1u8; 32]));

        // Differ only in the last byte
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        b[31] = 1;
        assert!(!constant_time_eq(&a, &b));

        // Differ only in the first byte
        a[0] = 1;
        b[31] = 0;
        assert!(!constant_time_eq(&a, &b));
    }

    #[test]
    fn test_different_length_arrays_return_false() {
        assert!(!constant_time_eq(b"short", b"longer"));
        assert!(!constant_time_eq(&[1, 2, 3], &[1, 2, 3, 4]));
        assert!(!constant_time_eq(&[1, 2, 3, 4], &[1, 2, 3]));
        assert!(!constant_time_eq(b"", b"notempty"));
        assert!(!constant_time_eq(b"notempty", b""));
    }

    #[test]
    fn test_empty_arrays_return_true() {
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq(&[], &[]));
    }

    #[test]
    fn test_single_byte_comparisons() {
        assert!(constant_time_eq(&[0], &[0]));
        assert!(constant_time_eq(&[255], &[255]));
        assert!(!constant_time_eq(&[0], &[1]));
        assert!(!constant_time_eq(&[255], &[0]));
    }

    #[test]
    fn test_typical_hmac_size() {
        // 32-byte HMAC (SHA-256)
        let hmac_a = [42u8; 32];
        let hmac_b = [42u8; 32];
        assert!(constant_time_eq(&hmac_a, &hmac_b));

        let mut hmac_c = [42u8; 32];
        hmac_c[15] = 99;
        assert!(!constant_time_eq(&hmac_a, &hmac_c));
    }
}
