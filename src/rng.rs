//! Random streams owned independently by wallets and storage providers, and
//! the OS draw for the values that never take a seed.

/// Fills `bytes` from the operating system's CSPRNG, with no stream between.
///
/// The auth nonces, request ids and key-file salts draw here, as the
/// reference draws them from the platform's `crypto.getRandomValues`
/// (`Random(n)`); no options field or constructor can seed them (#191).
pub(crate) fn os_fill(bytes: &mut [u8]) {
    use rand::RngCore;
    rand::rngs::OsRng.fill_bytes(bytes);
}

/// An owner-local stream, shared by its handles and never held across an await.
pub(crate) struct RandomSource {
    #[cfg(any(feature = "sqlite", feature = "mysql", test))]
    stream: std::sync::Mutex<rand::rngs::StdRng>,
}

impl RandomSource {
    pub(crate) fn new(seed: Option<[u8; 32]>) -> Self {
        #[cfg(not(any(feature = "sqlite", feature = "mysql", test)))]
        let _ = seed;

        Self {
            #[cfg(any(feature = "sqlite", feature = "mysql", test))]
            stream: {
                use rand::SeedableRng;
                std::sync::Mutex::new(match seed {
                    Some(seed) => rand::rngs::StdRng::from_seed(seed),
                    None => rand::rngs::StdRng::from_entropy(),
                })
            },
        }
    }

    #[cfg(any(feature = "sqlite", feature = "mysql", test))]
    pub(crate) fn fill_bytes(&self, bytes: &mut [u8]) {
        use rand::RngCore;
        // No caller runs code under this lock; a recovered stream can still
        // advance without reusing bytes or replacing its seed.
        self.stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fill_bytes(bytes);
    }

    #[cfg(any(feature = "sqlite", feature = "mysql"))]
    pub(crate) fn base64(&self, count: usize) -> String {
        let mut bytes = vec![0; count];
        self.fill_bytes(&mut bytes);
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn os_fill_draws_differ() {
        let (mut a, mut b) = ([0u8; 32], [0u8; 32]);
        super::os_fill(&mut a);
        super::os_fill(&mut b);
        assert_ne!(a, b);
    }
}
