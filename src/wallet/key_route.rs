//! The key route of the seven key calls and the two key-linkage reveals
//! (#186, #142, #569): a call with `privileged: true` goes to the wallet's
//! `PrivilegedKeyManager` with the caller's arguments intact, as
//! ts-stack@edf6e03 `Wallet.ts:455-570` sends it to
//! `this.privilegedKeyManager`; with none configured it is the
//! reference's refusal (`Wallet.ts:460-461`). Every other call stays with the
//! everyday `ProtoWallet`, its arguments intact as before.
//!
//! The arguments are validated before the route decides: the reference's
//! substrates run `validateWalletArgs` ahead of the wallet
//! (`WalletArgumentValidation.ts`, from `HTTPWalletJSON.ts:48`,
//! `WalletWireProcessor.ts:21`), so a privileged request with a bad argument
//! answers the validator's refusal, with or without a manager.

use super::*;
use bsv_rs::wallet::{
    DiscoverByAttributesArgs as AttributesArgs, DiscoverByIdentityKeyArgs as IdentityKeyArgs,
    DiscoverCertificatesResult as DiscoverResult,
};

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

/// The everyday `ProtoWallet`'s argument refusal, if its call gave one: its
/// validators run before any key is touched and are the only source of the
/// `InvalidParameter` class on the nine calls, so the everyday answer is
/// discarded and only the refusal kept. bsv-rs keeps the validators
/// themselves crate-private; the call is how they are reached.
fn argument_refusal<T>(everyday: bsv_rs::Result<T>) -> bsv_rs::Result<()> {
    match everyday {
        Err(
            refusal @ bsv_rs::Error::WalletRefusal {
                class: bsv_rs::WalletRefusalClass::InvalidParameter,
                ..
            },
        ) => Err(refusal),
        _ => Ok(()),
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

/// The seven key calls, each with its arguments and result: the list the
/// route, its `WalletInterface` impl and the tests' doubles are built from
/// (the two key-linkage reveals, #569, sit beside it at those three sites).
/// `key_calls!(then)` invokes `then!` on the list; `key_calls!(then { .. })`
/// puts the braced tokens ahead of it.
macro_rules! key_calls {
    ($then:ident $({ $($ahead:tt)* })?) => {
        $then! {
            $($($ahead)*)?
            get_public_key(GetPublicKeyArgs) -> GetPublicKeyResult;
            encrypt(EncryptArgs) -> EncryptResult;
            decrypt(DecryptArgs) -> DecryptResult;
            create_hmac(CreateHmacArgs) -> CreateHmacResult;
            verify_hmac(VerifyHmacArgs) -> VerifyHmacResult;
            create_signature(CreateSignatureArgs) -> CreateSignatureResult;
            verify_signature(VerifySignatureArgs) -> VerifySignatureResult;
        }
    };
}
// The tests' double is built from the same list.
#[cfg(all(test, feature = "sqlite"))]
pub(super) use key_calls;

// One routed method per key call: the manager for `privileged: true` once
// the everyday ProtoWallet's validators pass the arguments, the everyday
// ProtoWallet otherwise. `$everyday` names how the ProtoWallet is asked:
// `inherent` for the seven; `adapter` for the two reveals, whose validators
// sit behind the ProtoWallet's `WalletInterface` adapter, which checks the
// `privileged` and `privileged_reason` its inherent argument types omit.
macro_rules! routed_key_calls {
    ($everyday:ident; $($name:ident($args:ty) -> $result:ty;)*) => {
        $(
            pub(super) async fn $name(&self, args: $args) -> bsv_rs::Result<$result> {
                if args.privileged != Some(true) {
                    return $everyday!(self, $name, args);
                }
                argument_refusal($everyday!(self, $name, args.clone()))?;
                let manager = self.manager.ok_or_else(privileged_refusal)?;
                manager.$name(args, self.originator).await
            }
        )*
    };
}

macro_rules! inherent {
    ($route:expr, $name:ident, $args:expr) => {
        $route.proto.$name($args)
    };
}

macro_rules! adapter {
    ($route:expr, $name:ident, $args:expr) => {
        WalletInterface::$name($route.proto, $args, Some($route.originator)).await
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

    key_calls!(routed_key_calls { inherent; });

    routed_key_calls! {
        adapter;
        reveal_counterparty_key_linkage(InterfaceRevealCounterpartyArgs) -> RevealCounterpartyKeyLinkageResult;
        reveal_specific_key_linkage(InterfaceRevealSpecificArgs) -> RevealSpecificKeyLinkageResult;
    }
}

// The route as a `WalletInterface`, for bsv-rs's certificate helpers
// (`MasterCertificate::decrypt_fields`), which pass the caller's `privileged`
// to the wallet they are given, as the reference passes it to `this`
// (`Wallet.ts:705-724`): the seven key calls and the two reveals take the
// route, every other method is the everyday ProtoWallet's.
macro_rules! key_route_interface {
    (
        everyday { $(fn $name:ident(&self $(, $arg:ident: $arg_type:ty)*) -> $result:ty;)* }
        $($key:ident($key_args:ty) -> $key_result:ty;)*
    ) => {
        #[async_trait]
        impl WalletInterface for KeyRoute<'_> {
            $(
                async fn $key(
                    &self,
                    args: $key_args,
                    _originator: Option<&str>,
                ) -> bsv_rs::Result<$key_result> {
                    KeyRoute::$key(self, args).await
                }
            )*
            $(
                async fn $name(&self $(, $arg: $arg_type)*) -> $result {
                    WalletInterface::$name(self.proto $(, $arg)*).await
                }
            )*
        }
    };
}

key_calls!(key_route_interface {
    everyday {
        fn create_action(&self, args: CreateActionArgs, originator: Option<&str>) -> bsv_rs::Result<CreateActionResult>;
        fn sign_action(&self, args: SignActionArgs, originator: Option<&str>) -> bsv_rs::Result<SignActionResult>;
        fn abort_action(&self, args: AbortActionArgs, originator: Option<&str>) -> bsv_rs::Result<AbortActionResult>;
        fn list_actions(&self, args: ListActionsArgs, originator: Option<&str>) -> bsv_rs::Result<ListActionsResult>;
        fn internalize_action(&self, args: InternalizeActionArgs, originator: Option<&str>) -> bsv_rs::Result<InternalizeActionResult>;
        fn list_outputs(&self, args: ListOutputsArgs, originator: Option<&str>) -> bsv_rs::Result<ListOutputsResult>;
        fn relinquish_output(&self, args: RelinquishOutputArgs, originator: Option<&str>) -> bsv_rs::Result<RelinquishOutputResult>;
        fn acquire_certificate(&self, args: AcquireCertificateArgs, originator: Option<&str>) -> bsv_rs::Result<WalletCertificate>;
        fn list_certificates(&self, args: ListCertificatesArgs, originator: Option<&str>) -> bsv_rs::Result<ListCertificatesResult>;
        fn prove_certificate(&self, args: ProveCertificateArgs, originator: Option<&str>) -> bsv_rs::Result<ProveCertificateResult>;
        fn relinquish_certificate(&self, args: RelinquishCertificateArgs, originator: Option<&str>) -> bsv_rs::Result<RelinquishCertificateResult>;
        fn discover_by_identity_key(&self, args: IdentityKeyArgs, originator: Option<&str>) -> bsv_rs::Result<DiscoverResult>;
        fn discover_by_attributes(&self, args: AttributesArgs, originator: Option<&str>) -> bsv_rs::Result<DiscoverResult>;
        fn is_authenticated(&self, originator: Option<&str>) -> bsv_rs::Result<AuthenticatedResult>;
        fn wait_for_authentication(&self, originator: Option<&str>) -> bsv_rs::Result<AuthenticatedResult>;
        fn get_height(&self, originator: Option<&str>) -> bsv_rs::Result<GetHeightResult>;
        fn get_header_for_height(&self, args: GetHeaderArgs, originator: Option<&str>) -> bsv_rs::Result<GetHeaderResult>;
        fn get_network(&self, originator: Option<&str>) -> bsv_rs::Result<GetNetworkResult>;
        fn get_version(&self, originator: Option<&str>) -> bsv_rs::Result<GetVersionResult>;
    }
    reveal_counterparty_key_linkage(InterfaceRevealCounterpartyArgs) -> RevealCounterpartyKeyLinkageResult;
    reveal_specific_key_linkage(InterfaceRevealSpecificArgs) -> RevealSpecificKeyLinkageResult;
});
