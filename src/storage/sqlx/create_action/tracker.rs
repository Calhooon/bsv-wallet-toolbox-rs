use crate::error::{Error, Result};
use bsv_rs::transaction::{Beef, ChainTracker};
use std::collections::{HashMap, HashSet};

// =============================================================================
// Helper Functions
// =============================================================================

/// Read every root of the caller's `inputBEEF` against `tracker`, before
/// the write lock. Returns the confirmed `(height, root)` pairs; a refused
/// root, or a tracker fault, fails the call with the error the assembly's
/// own verification gives. Bytes that do not parse, or a bump whose root
/// does not compute, are left to the assembly to refuse.
pub(super) async fn confirm_caller_beef_roots(
    tracker: Option<&dyn ChainTracker>,
    input_beef: Option<&[u8]>,
) -> Result<HashSet<(u32, String)>> {
    let mut confirmed = HashSet::new();
    let (Some(tracker), Some(bytes)) = (tracker, input_beef) else {
        return Ok(confirmed);
    };
    if bytes.is_empty() {
        return Ok(confirmed);
    }
    let Ok(beef) = Beef::from_binary(bytes) else {
        return Ok(confirmed);
    };
    let roots: Vec<(u32, String)> = beef
        .bumps
        .iter()
        .filter_map(|b| b.compute_root(None).ok().map(|r| (b.block_height, r)))
        .collect();
    for (height, root) in roots {
        if confirmed.contains(&(height, root.clone())) {
            continue;
        }
        match tracker.is_valid_root_for_height(&root, height).await {
            Ok(true) => {
                confirmed.insert((height, root));
            }
            Ok(false) => {
                return Err(Error::ValidationError(format!(
                    "inputBEEF: invalid merkle root {} at height {}",
                    root, height
                )));
            }
            Err(e) => {
                return Err(Error::ValidationError(format!(
                    "inputBEEF: failed to verify merkle root at height {}: {}",
                    height, e
                )));
            }
        }
    }
    Ok(confirmed)
}

/// Every answer the chain tracker gave this call before the write lock, by
/// `(height, root)`: `Ok(verdict)`, or the fault's text.
pub(super) struct TrackerAnswers(
    std::sync::Mutex<HashMap<(u32, String), std::result::Result<bool, String>>>,
);

impl TrackerAnswers {
    /// Seeded with the caller's roots, confirmed before anything else.
    pub(super) fn confirmed(roots: HashSet<(u32, String)>) -> Self {
        Self(std::sync::Mutex::new(
            roots.into_iter().map(|key| (key, Ok(true))).collect(),
        ))
    }

    fn get(&self, height: u32, root: &str) -> Option<std::result::Result<bool, String>> {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(height, root.to_string()))
            .cloned()
    }

    fn put(&self, height: u32, root: &str, answer: std::result::Result<bool, String>) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert((height, root.to_string()), answer);
    }
}

/// The pre-check's tracker: asks `inner` (with no lock held) and keeps every
/// answer. A root already confirmed this call is not asked again.
pub(super) struct RecordingTracker<'a> {
    pub(super) inner: &'a dyn ChainTracker,
    pub(super) answers: &'a TrackerAnswers,
}

#[async_trait::async_trait]
impl ChainTracker for RecordingTracker<'_> {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
        if let Some(Ok(true)) = self.answers.get(height, root) {
            return Ok(true);
        }
        let answer = self.inner.is_valid_root_for_height(root, height).await;
        self.answers.put(
            height,
            root,
            answer.as_ref().map(|v| *v).map_err(ToString::to_string),
        );
        answer
    }

    async fn current_height(
        &self,
    ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
        self.inner.current_height().await
    }
}

/// The write-locked step's tracker: answers from the pre-check's answers
/// only, never asking anyone. A root the pre-check did not read answers a
/// fault and is remembered, so the step rolls back and checks again.
pub(super) struct CachedTracker<'a> {
    pub(super) answers: &'a TrackerAnswers,
    pub(super) missed: std::sync::Mutex<Option<(u32, String)>>,
}

impl<'a> CachedTracker<'a> {
    pub(super) fn new(answers: &'a TrackerAnswers) -> Self {
        Self {
            answers,
            missed: std::sync::Mutex::new(None),
        }
    }

    /// The first root asked that the pre-check had not read.
    pub(super) fn missed(&self) -> Option<(u32, String)> {
        self.missed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

#[async_trait::async_trait]
impl ChainTracker for CachedTracker<'_> {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
        use bsv_rs::transaction::ChainTrackerError;
        match self.answers.get(height, root) {
            Some(Ok(verdict)) => Ok(verdict),
            Some(Err(fault)) => Err(ChainTrackerError::Other(fault)),
            None => {
                self.missed
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get_or_insert_with(|| (height, root.to_string()));
                Err(ChainTrackerError::Other(format!(
                    "root {} at height {} was not read before the write lock",
                    root, height
                )))
            }
        }
    }

    async fn current_height(
        &self,
    ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Err(bsv_rs::transaction::ChainTrackerError::Other(
            "the chain tracker is not asked under the write lock".to_string(),
        ))
    }
}
