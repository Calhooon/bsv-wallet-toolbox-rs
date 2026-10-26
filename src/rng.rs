//! Random streams owned independently by wallets and storage providers.

/// An owner-local stream, shared by its handles and never held across an await.
pub(crate) struct RandomSource(std::sync::Mutex<rand::rngs::StdRng>);

impl RandomSource {
    pub(crate) fn new(seed: Option<[u8; 32]>) -> Self {
        use rand::SeedableRng;
        Self(std::sync::Mutex::new(match seed {
            Some(seed) => rand::rngs::StdRng::from_seed(seed),
            None => rand::rngs::StdRng::from_entropy(),
        }))
    }

    pub(crate) fn fill_bytes(&self, bytes: &mut [u8]) {
        use rand::RngCore;
        // No caller runs code under this lock; a recovered stream can still
        // advance without reusing bytes or replacing its seed.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fill_bytes(bytes);
    }

    pub(crate) fn base64(&self, count: usize) -> String {
        let mut bytes = vec![0; count];
        self.fill_bytes(&mut bytes);
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
    }
}
