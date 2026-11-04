//! The permission request flow and the host's answer (#751 step 2a): the
//! request queued under its request id, the handler told, and the answer
//! by [`WalletPermissionsManager::grant_permission`] or
//! [`WalletPermissionsManager::deny_permission`], as the reference's
//! `requestPermissionFlow`, `grantPermission` and `denyPermission`
//! (ts-stack@edf6e03 `WalletPermissionsManager.ts:2550-2668`, `:1160-1205`).
//! A handler that answers inline with a token is read as a persistent grant
//! with the token's expiry and, for spending, its amount.

use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bsv_rs::primitives::ec::PublicKey;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{CreateSignatureArgs, EncryptArgs};
use tokio::sync::{oneshot, watch};

use super::{
    normalize_originator, permission_token_protocol, proto_wallet_refuses, Counterparty, Error,
    PermissionRequest, PermissionToken, PermissionType, PushDrop, Result, TokenQuery,
    WalletPermissionsManager,
};

/// The window in which a fresh grant admits and skips a duplicate mint
/// (`RECENT_GRANT_COVER_MS`, `:748`).
const RECENT_GRANT_COVER: Duration = Duration::from_secs(15);

/// A request awaiting the host's answer and every caller waiting on it.
pub(super) struct ActiveRequest {
    request: PermissionRequest,
    pending: Vec<oneshot::Sender<Result<bool>>>,
}

/// The request a handler's future answers and, once the handler grants it
/// inline, the end of that grant: the caller polls the handler to it, so
/// the mint is not dropped with the handler's future.
struct Answering {
    key: String,
    granting: StdMutex<Option<watch::Receiver<()>>>,
}

tokio::task_local! {
    /// Set while the caller polls the handler's future.
    static ANSWERING: Arc<Answering>;
}

/// When the handler's own future grants `request_id`, the mark its caller
/// waits on until the grant ends; none for any other grant.
fn inline_grant(request_id: &str) -> Option<watch::Sender<()>> {
    let mark = |answering: &Arc<Answering>| {
        (answering.key == request_id).then(|| {
            let (granting, ended) = watch::channel(());
            *locked(&answering.granting) = Some(ended);
            granting
        })
    };
    ANSWERING.try_with(mark).ok().flatten()
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

/// A mint's in-flight mark (`:1133-1144`), cleared however the mint ends,
/// a cancellation included; its sender drops after, waking the waiters.
struct InFlight<'a> {
    mints: &'a StdMutex<std::collections::HashMap<String, watch::Receiver<()>>>,
    key: &'a str,
    _minting: watch::Sender<()>,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        locked(self.mints).remove(self.key);
    }
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
    /// with that id, while the handler runs or after it returns (a handler
    /// may stay pending; the caller returns at the answer, the handler's
    /// future dropped, save that a persistent grant made inside the
    /// handler's own future is polled to its mint's end); a handler that
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
            if let Some(queue) = active.get_mut(&key) {
                // A caller dropped while it waited (a disconnect) answers
                // nobody; a queue of those alone is asked again.
                queue.pending.retain(|caller| !caller.is_closed());
            }
            if let Some(queue) = active.get_mut(&key).filter(|q| !q.pending.is_empty()) {
                queue.pending.push(sender);
                true
            } else {
                request.request_id = Some(key.clone());
                let queued = ActiveRequest {
                    request: request.clone(),
                    pending: vec![sender],
                };
                active.insert(key.clone(), queued);
                false
            }
        };
        if joined {
            return answer(waiting).await;
        }
        let answering = Arc::new(Answering {
            key: key.clone(),
            granting: StdMutex::new(None),
        });
        let handled = ANSWERING.scope(Arc::clone(&answering), handler(request));
        let mut handled = std::pin::pin!(handled);
        let mut waiting = waiting;
        let answered = tokio::select! {
            answered = &mut waiting => {
                let granting = locked(&answering.granting).take();
                if let Some(mut ended) = granting {
                    // The handler granted inline: its mint runs to its end.
                    tokio::select! {
                        _ = &mut handled => {}
                        _ = ended.changed() => {}
                    }
                }
                return answered.unwrap_or_else(|_| Err(denied()));
            }
            answered = &mut handled => answered,
        };
        let unanswered = locked(&self.active_requests).contains_key(&key);
        if unanswered {
            self.answer_inline(&key, answered).await?;
        }
        answer(waiting).await
    }

    /// A handler's own answer to the request `key` names: a token grants
    /// it persistently with the token's expiry and amount, an error refuses
    /// every waiting caller and is returned.
    async fn answer_inline(&self, key: &str, answered: Result<PermissionToken>) -> Result<()> {
        match answered {
            Ok(token) => {
                let (expiry, amount) = (Some(token.expiry), token.authorized_amount);
                // The callers are admitted before the grant persists
                // (`:1171-1180`); a failed persist is the host's to hear,
                // and an inline host has no other ear than the log.
                if let Err(error) = self.grant_permission(key, expiry, false, amount).await {
                    tracing::warn!(%error, "permission granted but not persisted");
                }
                Ok(())
            }
            Err(error) => {
                let queue = locked(&self.active_requests).remove(key);
                for caller in queue.into_iter().flat_map(|queue| queue.pending) {
                    let _ = caller.send(Err(Error::AccessDenied(error.to_string())));
                }
                Err(error)
            }
        }
    }

    /// Grants the request `request_id` names (`grantPermission`,
    /// `:1160-1180`): every caller waiting on it is admitted; an ephemeral
    /// grant ends there, nothing cached or stored (`:1179`, `:304-305`);
    /// else the grant persists with `expiry` (UNIX seconds, 0 or none for
    /// never) and, for spending, `amount` (the request's satoshis when
    /// none).
    ///
    /// # Errors
    ///
    /// `Request ID not found.` for an id with no request waiting; a failed
    /// mint, after the callers are admitted, with nothing cached.
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
        if ephemeral {
            for caller in matching.pending {
                let _ = caller.send(Ok(true));
            }
            return Ok(());
        }
        // Held to the grant's end: a handler granting inside its own
        // future keeps its caller polling it until the mint is done.
        let _granting = inline_grant(request_id);
        for caller in matching.pending {
            let _ = caller.send(Ok(true));
        }
        self.grant_persistent_permission(&matching.request, expiry.unwrap_or(0), amount)
            .await
    }

    /// Denies the request `request_id` names (`denyPermission`,
    /// `:1189-1205`): every caller waiting on it is refused with
    /// `Permission denied.`; nothing is stored.
    ///
    /// # Errors
    ///
    /// `Request ID not found.` for an id with no request waiting.
    pub fn deny_permission(&self, request_id: &str) -> Result<()> {
        let matching = locked(&self.active_requests)
            .remove(request_id)
            .ok_or_else(|| Error::InvalidArgument("Request ID not found.".to_string()))?;
        for caller in matching.pending {
            let _ = caller.send(Err(denied()));
        }
        Ok(())
    }

    /// `grantPersistentPermission` (`:1104-1149`): the mint, skipped for
    /// a request whose grant is cached, inside its cover or minting, never
    /// for a renewal (`:1112-1124`), the in-flight mint awaited before; a
    /// renewal to `renew_permission_on_chain` (`:1127-1128`), else
    /// `create_permission_on_chain` (`:1129`), published in flight while it
    /// runs (`:1133-1144`); then the five-minute cache entry (`:1147`) and
    /// the recent-grant mark (`:1148`). A failed mint is returned, nothing
    /// cached. A spending grant is cached and mints nothing until #751 lane
    /// c: the port reads no spending token yet, so the reference's uncached
    /// spending mint (`:1112-1115`, `:1147`) would prompt every spend and
    /// leave a token nothing reads; its mint lands with its lookup.
    async fn grant_persistent_permission(
        &self,
        request: &PermissionRequest,
        expiry: u64,
        amount: Option<u64>,
    ) -> Result<()> {
        let key = Self::request_key(request);
        if request.request_type == PermissionType::Spending {
            self.cache_permission(key, expiry).await;
            return Ok(());
        }
        let renewal = request.renewal == Some(true);
        let in_flight = locked(&self.mints_in_flight).get(&key).cloned();
        let mut skip = !renewal
            && (self.is_permission_cached(&key).await
                || self.is_recently_granted(&key)
                || in_flight.is_some());
        if let (true, Some(mut minting)) = (skip, in_flight) {
            // The sender drops when the mint ends; only a mint that landed skips.
            let _ = minting.changed().await;
            skip = self.is_permission_cached(&key).await || self.is_recently_granted(&key);
        }
        if !skip {
            let (minting, waiting) = watch::channel(());
            locked(&self.mints_in_flight).insert(key.clone(), waiting);
            let mark = InFlight {
                mints: &self.mints_in_flight,
                key: &key,
                _minting: minting,
            };
            let minted = match (renewal, request.previous_token.as_ref()) {
                (true, Some(previous)) => {
                    Self::renew_permission_on_chain(previous, request, expiry, amount)
                }
                _ => {
                    self.create_permission_on_chain(request, expiry, amount)
                        .await
                }
            };
            drop(mark);
            minted?;
        }
        self.cache_permission(key.clone(), expiry).await;
        self.mark_recent_grant(key);
        Ok(())
    }

    /// `hasRecentOrPendingGrantCore` (`:5652-5658`): cached or inside the
    /// cover, else, with a mint in flight, the same once it ends.
    pub(super) async fn has_recent_or_pending_grant(&self, key: &str) -> bool {
        if self.is_permission_cached(key).await || self.is_recently_granted(key) {
            return true;
        }
        let in_flight = locked(&self.mints_in_flight).get(key).cloned();
        let Some(mut minting) = in_flight else {
            return false;
        };
        let _ = minting.changed().await;
        self.is_permission_cached(key).await || self.is_recently_granted(key)
    }

    /// `markRecentGrant` (`:5684-5690`); no spending grant reaches it.
    fn mark_recent_grant(&self, key: String) {
        locked(&self.recent_grants).insert(key, Instant::now() + RECENT_GRANT_COVER);
    }

    /// `isRecentlyGranted` (`:5692-5700`).
    fn is_recently_granted(&self, key: &str) -> bool {
        let mut recent = locked(&self.recent_grants);
        match recent.get(key) {
            Some(end) if Instant::now() <= *end => true,
            Some(_) => {
                recent.remove(key);
                false
            }
            None => false,
        }
    }

    /// `renewPermissionOnChain` (`:3634-3700`) is #751 lane b's: it spends
    /// the previous token, which needs the token input signer. Until then a
    /// renewal mints nothing and is refused; the manager raises none yet.
    fn renew_permission_on_chain(
        _previous: &PermissionToken,
        _request: &PermissionRequest,
        _expiry: u64,
        _amount: Option<u64>,
    ) -> Result<()> {
        Err(Error::InvalidOperation(
            "Permission token renewal is not ported yet (#751).".to_string(),
        ))
    }

    /// `createPermissionOnChain` (`:3349-3392`): the request's originator
    /// normalized; the kind's fields (`buildPushdropFields`, `:3708-3749`),
    /// each encrypted to this wallet under the token protocol as the admin
    /// originator (`encryptPermissionTokenField`, `:2722-2737`); the
    /// `PushDrop.lock(fields, protocol, '1', 'self', true, true)` script
    /// (`:3360-3367`); the kind's tags (`buildTagsForRequest`, `:3755-3787`),
    /// trimmed and lowercased as the reference's `createAction` stores them
    /// (the step 1 lookup's `TokenQuery::tags`); one 1-satoshi output in the
    /// kind's admin basket, no explicit input, `acceptDelayedBroadcast`,
    /// through the underlying wallet's `create_action` as the admin
    /// originator (`completePermissionTokenAction`, `:3566-3576`).
    pub(super) async fn create_permission_on_chain(
        &self,
        request: &PermissionRequest,
        expiry: u64,
        amount: Option<u64>,
    ) -> Result<()> {
        let originator = normalize_originator(&request.originator);
        let (basket, texts, tags) = mint_plan(request, &originator, expiry, amount)?;
        let mut fields = Vec::with_capacity(texts.len() + 1);
        for text in texts {
            fields.push(self.encrypt_token_text(text).await?);
        }
        let script = self.token_lock(fields).await?;
        let kind = kind_name(request.request_type);
        let args = serde_json::json!({
            "description": format!("Grant {kind} permission"),
            "outputs": [{
                "lockingScript": script.to_hex(),
                "satoshis": 1,
                "outputDescription": format!("{kind} permission token"),
                "basket": basket,
                "tags": tags,
            }],
            "options": { "acceptDelayedBroadcast": true },
        });
        let args = serde_json::from_value(args)?;
        match self
            .underlying
            .create_action(args, Some(&self.admin_originator))
            .await
        {
            Ok(_) => Ok(()),
            // An underlying that keeps no outputs holds no token, as the
            // lookup reads it (`cannot_list`): the grant stands on the cache.
            Err(error) if cannot_create(&error) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// `encryptPermissionTokenField` (`:2722-2737`): keyID `"1"`, the
    /// counterparty `self` by default.
    async fn encrypt_token_text(&self, text: String) -> Result<Vec<u8>> {
        let args = EncryptArgs {
            plaintext: text.into_bytes(),
            protocol_id: permission_token_protocol(),
            key_id: "1".to_string(),
            counterparty: None,
            ..Default::default()
        };
        let encrypted = self
            .underlying
            .encrypt(args, Some(&self.admin_originator))
            .await?;
        Ok(encrypted.ciphertext)
    }

    /// The sdk's `PushDrop.lock` with `forSelf` and `includeSignature`
    /// true, the lock before the fields: the token protocol's key for
    /// `self`, the signature under it over the fields joined, pushed last.
    async fn token_lock(&self, mut fields: Vec<Vec<u8>>) -> Result<LockingScript> {
        let key = self.token_locking_key().await.ok_or_else(|| {
            Error::AccessDenied(
                "Wallet returned an invalid permission-token locking key.".to_string(),
            )
        })?;
        let key = PublicKey::from_hex(key)?;
        let args = CreateSignatureArgs {
            data: Some(fields.concat()),
            protocol_id: permission_token_protocol(),
            key_id: "1".to_string(),
            counterparty: Some(Counterparty::Self_),
            ..Default::default()
        };
        let signed = self
            .underlying
            .create_signature(args, Some(&self.admin_originator))
            .await?;
        fields.push(signed.signature);
        Ok(PushDrop::new(key, fields).lock())
    }
}

/// Whether a create failure is an underlying that cannot hold a token.
fn cannot_create(error: &bsv_rs::Error) -> bool {
    proto_wallet_refuses(error, "createAction")
}

/// The reference's `r.type`, as its description and output name it.
fn kind_name(kind: PermissionType) -> &'static str {
    match kind {
        PermissionType::Protocol => "protocol",
        PermissionType::Basket => "basket",
        PermissionType::Certificate => "certificate",
        PermissionType::Spending => "spending",
    }
}

/// The admin basket a spending token is minted in (`BASKET_MAP`, `:422`).
const SPENDING_TOKEN_BASKET: &str = "admin spending-authorization";

/// The mint's basket, plaintext fields and stored tags for `request`;
/// spending's is held for #751 lane c, which mints it.
fn mint_plan(
    request: &PermissionRequest,
    originator: &str,
    expiry: u64,
    amount: Option<u64>,
) -> Result<(&'static str, Vec<String>, Vec<String>)> {
    if request.request_type == PermissionType::Spending {
        let requested = request.spending.as_ref().map_or(0, |s| s.satoshis);
        let texts = vec![
            originator.to_string(),
            amount.unwrap_or(requested).to_string(),
        ];
        let tags = vec![format!("originator {originator}").trim().to_lowercase()];
        return Ok((SPENDING_TOKEN_BASKET, texts, tags));
    }
    let query = token_query(request)?;
    let texts = query.plaintexts(originator, expiry)?;
    Ok((query.basket(), texts, query.tags(Some(originator))))
}

/// What a token minted for `request`, of a kind the lookup reads, says.
fn token_query(request: &PermissionRequest) -> Result<TokenQuery<'_>> {
    let missing = |what: &str| Error::InvalidArgument(format!("A grant names no {what}."));
    let privileged = request.privileged == Some(true);
    Ok(match request.request_type {
        PermissionType::Protocol => TokenQuery::Protocol {
            privileged,
            protocol: request
                .protocol_id
                .as_ref()
                .ok_or_else(|| missing("protocol"))?,
            counterparty: request.counterparty.as_deref().unwrap_or("self"),
        },
        PermissionType::Basket => TokenQuery::Basket {
            basket: request.basket.as_deref().ok_or_else(|| missing("basket"))?,
        },
        PermissionType::Certificate => {
            let details = request
                .certificate
                .as_ref()
                .ok_or_else(|| missing("certificate"))?;
            TokenQuery::Certificate {
                privileged,
                verifier: &details.verifier,
                cert_type: &details.cert_type,
                fields: &details.fields,
            }
        }
        PermissionType::Spending => return Err(missing("token the lookup reads")),
    })
}

impl TokenQuery<'_> {
    /// `buildPushdropFields`' plaintexts (`:3708-3749`): the counterparty
    /// at level 2 alone, empty below it.
    fn plaintexts(&self, domain: &str, expiry: u64) -> Result<Vec<String>> {
        let mut texts = vec![domain.to_string(), expiry.to_string()];
        match self {
            Self::Protocol {
                privileged,
                protocol,
                counterparty,
            } => {
                let level = protocol.security_level.as_u8();
                let counterparty = if level == 2 { *counterparty } else { "" };
                texts.push(privileged.to_string());
                texts.push(level.to_string());
                texts.push(protocol.protocol_name.clone());
                texts.push(counterparty.to_string());
            }
            Self::Basket { basket } => texts.push((*basket).to_string()),
            Self::Certificate {
                privileged,
                verifier,
                cert_type,
                fields,
            } => {
                texts.push(privileged.to_string());
                texts.push((*cert_type).to_string());
                texts.push(serde_json::to_string(fields)?);
                texts.push((*verifier).to_string());
            }
        }
        Ok(texts)
    }
}
