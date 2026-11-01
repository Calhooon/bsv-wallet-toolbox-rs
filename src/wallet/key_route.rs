//! The key route of the seven key calls (#186, #142): a call with
//! `privileged: true` goes to the wallet's `PrivilegedKeyManager` with the
//! caller's arguments intact, as ts-stack@edf6e03 `Wallet.ts:455-570` sends
//! it to `this.privilegedKeyManager`; with none configured it is the
//! reference's refusal (`Wallet.ts:460-461`). Every other call stays with the
//! everyday `ProtoWallet`, its arguments intact as before.

use super::*;

/// The reference's text for a privileged request on a wallet with no
/// privileged key manager (ts-stack@edf6e03 `Wallet.ts:461`).
pub(super) const PRIVILEGED_KEY_MANAGER_REQUIRED: &str =
    "Privileged operations require the Wallet to be configured with a privileged key manager.";

/// The refusal of a privileged request with no manager: the reference throws
/// a plain `Error`; here it is the `NotImplemented` class, which the hosts
/// map to one status, and the reference's text byte for byte.
pub(super) fn privileged_refusal() -> bsv_rs::Error {
    bsv_rs::Error::WalletRefusal {
        class: bsv_rs::WalletRefusalClass::NotImplemented,
        message: PRIVILEGED_KEY_MANAGER_REQUIRED.to_string(),
    }
}

/// The keys one call is answered under: the everyday `ProtoWallet`, or the
/// privileged key manager when the call asks for it.
#[derive(Clone, Copy)]
pub(super) struct KeyRoute<'a> {
    proto: &'a ProtoWallet,
    manager: Option<&'a dyn PrivilegedKeyManager>,
    originator: &'a str,
}

// One routed method per key call: the manager for `privileged: true`, the
// everyday ProtoWallet otherwise.
macro_rules! routed_key_calls {
    ($($name:ident($args:ty) -> $result:ty;)*) => {
        $(
            pub(super) async fn $name(&self, args: $args) -> bsv_rs::Result<$result> {
                match self.manager(args.privileged)? {
                    Some(manager) => manager.$name(args, self.originator).await,
                    None => self.proto.$name(args),
                }
            }
        )*
    };
}

impl<'a> KeyRoute<'a> {
    pub(super) fn new(
        proto: &'a ProtoWallet,
        manager: Option<&'a Arc<dyn PrivilegedKeyManager>>,
        originator: Option<&'a str>,
    ) -> Self {
        Self {
            proto,
            manager: manager.map(|manager| manager.as_ref()),
            originator: originator.unwrap_or_default(),
        }
    }

    /// The manager a privileged request goes to, `None` for an everyday one,
    /// or the reference's refusal when the request is privileged and the
    /// wallet has no manager.
    pub(super) fn manager(
        &self,
        privileged: Option<bool>,
    ) -> bsv_rs::Result<Option<&'a dyn PrivilegedKeyManager>> {
        if privileged != Some(true) {
            return Ok(None);
        }
        self.manager.map(Some).ok_or_else(privileged_refusal)
    }

    routed_key_calls! {
        get_public_key(GetPublicKeyArgs) -> GetPublicKeyResult;
        encrypt(EncryptArgs) -> EncryptResult;
        decrypt(DecryptArgs) -> DecryptResult;
        create_hmac(CreateHmacArgs) -> CreateHmacResult;
        verify_hmac(VerifyHmacArgs) -> VerifyHmacResult;
        create_signature(CreateSignatureArgs) -> CreateSignatureResult;
        verify_signature(VerifySignatureArgs) -> VerifySignatureResult;
    }
}
