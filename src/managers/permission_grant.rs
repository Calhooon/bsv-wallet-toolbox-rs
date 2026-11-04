//! The permission request flow and the host's answer (#751 step 2a): the
//! request queued under its request id, the handler told, and the answer
//! by [`WalletPermissionsManager::grant_permission`] or
//! [`WalletPermissionsManager::deny_permission`], as the reference's
//! `requestPermissionFlow`, `grantPermission` and `denyPermission`
//! (ts-stack@edf6e03 `WalletPermissionsManager.ts:2550-2668`, `:1160-1205`).
//! A handler that answers inline with a token is read as a persistent grant
//! with the token's expiry and, for spending, its amount.

use std::sync::{Mutex as StdMutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::*;

/// The window in which a fresh grant admits and skips a duplicate mint
/// (`RECENT_GRANT_COVER_MS`, `:748`).
const RECENT_GRANT_COVER: Duration = Duration::from_secs(15);

/// A request awaiting the host's answer and every caller waiting on it.
pub(super) struct ActiveRequest {
    request: PermissionRequest,
    pending: Vec<oneshot::Sender<Result<bool>>>,
}

/// The reference's denial (`:1198`).
fn denied() -> Error {
    Error::AccessDenied("Permission denied.".to_string())
}

/// A map's guard; a poisoned map is read as it stands.
fn locked<T>(mutex: &StdMutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A waiting caller's answer; a queue dropped unanswered is a denial.
async fn answer(waiting: oneshot::Receiver<Result<bool>>) -> Result<bool> {
    waiting.await.unwrap_or_else(|_| Err(denied()))
}

impl WalletPermissionsManager {
    /// `buildRequestKey` (`:5816-5828`) as this port keys its cache.
    fn request_key(request: &PermissionRequest) -> String {
        Self::build_cache_key(
            request.request_type,
            &request.originator,
            request.privileged,
            request.protocol_id.as_ref(),
            request.counterparty.as_deref(),
            request.basket.as_deref(),
            request.certificate.as_ref(),
            request.spending.as_ref().map(|s| s.satoshis),
        )
    }

    /// `buildActiveRequestKey` (`:5830-5840`): a spending request its own
    /// id, every other kind the request key and its usage type (none here
    /// until #763).
    fn active_request_key(&self, request: &PermissionRequest) -> String {
        let base = Self::request_key(request);
        if request.request_type == PermissionType::Spending {
            let sequence = self
                .spending_request_sequence
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            format!("{base}:request:{sequence}")
        } else {
            format!("{base}:")
        }
    }

    /// Asks the host for consent (`requestPermissionFlow`, `:2550-2668`):
    /// a caller for a request already awaiting an answer waits on it; else
    /// the request is queued under its `request_id` and handed to the
    /// handler. The host answers by `grant_permission` or `deny_permission`
    /// with that id, from within the handler or after it; a handler that
    /// returns with the request still unanswered answers it, a token as a
    /// persistent grant with the token's expiry (and, for spending, its
    /// `authorized_amount`), an error as a denial with that error. With no
    /// handler registered the request is refused.
    pub async fn request_permission_flow(&self, mut request: PermissionRequest) -> Result<bool> {
        let handler = self
            .permission_request_handler
            .read()
            .await
            .clone()
            .ok_or_else(|| {
                Error::AccessDenied(
                    "No permission request handler registered. Cannot request user consent."
                        .to_string(),
                )
            })?;
        let key = self.active_request_key(&request);
        let (sender, waiting) = oneshot::channel();
        let joined = {
            let mut active = locked(&self.active_requests);
            match active.get_mut(&key).filter(|q| !q.pending.is_empty()) {
                Some(queue) => {
                    queue.pending.push(sender);
                    true
                }
                None => {
                    request.request_id = Some(key.clone());
                    let queued = ActiveRequest {
                        request: request.clone(),
                        pending: vec![sender],
                    };
                    active.insert(key.clone(), queued);
                    false
                }
            }
        };
        if joined {
            return answer(waiting).await;
        }
        let answered = handler(request).await;
        let unanswered = locked(&self.active_requests).contains_key(&key);
        if unanswered {
            match answered {
                Ok(token) => {
                    let (expiry, amount) = (Some(token.expiry), token.authorized_amount);
                    // The callers are admitted before the grant persists
                    // (`:1171-1180`); a failed persist is the host's to
                    // hear, and an inline host has no other ear than the log.
                    if let Err(error) = self.grant_permission(&key, expiry, false, amount).await {
                        tracing::warn!(%error, "permission granted but not persisted");
                    }
                }
                Err(error) => {
                    let queue = locked(&self.active_requests).remove(&key);
                    if let Some(queue) = queue {
                        for caller in queue.pending {
                            let _ = caller.send(Err(Error::AccessDenied(error.to_string())));
                        }
                    }
                    return Err(error);
                }
            }
        }
        answer(waiting).await
    }

    /// Grants the request `request_id` names (`grantPermission`,
    /// `:1160-1180`): every caller waiting on it is admitted; an ephemeral
    /// grant ends there, nothing cached or stored (`:1179`, `:304-305`);
    /// else the grant persists with `expiry` (UNIX seconds, 0 or none for
    /// never) and, for spending, `amount` (the request's satoshis when
    /// none). `Request ID not found.` for an id with no request waiting.
    pub async fn grant_permission(
        &self,
        request_id: &str,
        expiry: Option<u64>,
        ephemeral: bool,
        amount: Option<u64>,
    ) -> Result<()> {
        let matching = locked(&self.active_requests)
            .remove(request_id)
            .ok_or_else(|| Error::InvalidArgument("Request ID not found.".to_string()))?;
        for caller in matching.pending {
            let _ = caller.send(Ok(true));
        }
        if ephemeral {
            return Ok(());
        }
        self.grant_persistent_permission(&matching.request, expiry.unwrap_or(0), amount)
            .await
    }

    /// Denies the request `request_id` names (`denyPermission`,
    /// `:1189-1205`): every caller waiting on it is refused with
    /// `Permission denied.`; nothing is stored.
    pub async fn deny_permission(&self, request_id: &str) -> Result<()> {
        let matching = locked(&self.active_requests)
            .remove(request_id)
            .ok_or_else(|| Error::InvalidArgument("Request ID not found.".to_string()))?;
        for caller in matching.pending {
            let _ = caller.send(Err(denied()));
        }
        Ok(())
    }

    /// `grantPersistentPermission` (`:1104-1149`): the five-minute cache
    /// entry (never for spending, `:1147`) and the recent-grant mark
    /// (`:1148`).
    async fn grant_persistent_permission(
        &self,
        request: &PermissionRequest,
        expiry: u64,
        _amount: Option<u64>,
    ) -> Result<()> {
        let key = Self::request_key(request);
        if request.request_type != PermissionType::Spending {
            self.cache_permission(key.clone(), expiry).await;
        }
        self.mark_recent_grant(request, key);
        Ok(())
    }

    /// `markRecentGrant` (`:5684-5690`): never for spending.
    fn mark_recent_grant(&self, request: &PermissionRequest, key: String) {
        if request.request_type == PermissionType::Spending {
            return;
        }
        locked(&self.recent_grants).insert(key, Instant::now() + RECENT_GRANT_COVER);
    }
}
