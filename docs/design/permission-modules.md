# Permission-module delegation on the seven key calls, as the reference does it (#726)

A design note for the keeper and the owner, no code. Labels: `[X]` read at the pin named; `[D]`
told (the issue, the brief). Nothing here was run: every claim is a reading, with its lines.
Nothing is decided by this note.

Pins: ts-stack `edf6e03` (`~/bsv/ts-stack`), the file cited as `WPM.ts` is
`packages/wallet/wallet-toolbox/src/WalletPermissionsManager.ts`, its module tests as
`pmodules.test.ts` (`packages/wallet/wallet-toolbox/src/__tests/WalletPermissionsManager.pmodules.test.ts`);
bsv-rs `0.9.0-beta.6` (the crate's pin, read from the cargo registry); this crate at beta
`0a036015`. Asked by the keeper's 2026-11-03 19:0xZ reading before any lane on #726 `[D]`.

## 1. The reference's module interface and delegation

### 1.1 The interface

`WPM.ts:178-230` `[X]`:

- `:179-183` `PermissionsModuleRequest { method: string, args: object, originator: string }`.
  `method` is the BRC-100 method's camelCase name (`'encrypt'`, `'createSignature'`, ...);
  `args` is the caller's argument object as received, untouched.
- `:189` `PermissionsModuleNext = (args: object) => Promise<unknown>`: the underlying wallet
  call with the module's arguments.
- `:195-230` `PermissionsModule`, three members:
  - `:205` `handleRequest?(req, next)`, optional: the module owns the whole call. It may call
    `next` once, or answer a result of its own without the wallet. When present, `onRequest` and
    `onResponse` are not called (`:197-204`).
  - `:214` `onRequest(req) => Promise<{ args: object }>`, required: the arguments the underlying
    wallet is called with. A module refuses by throwing (`:208-209`: "Can check and enforce
    permissions, throw errors, or modify any arguments").
  - `:223-229` `onResponse(res, { method, originator }) => Promise<any>`, required: the result
    the caller receives.

### 1.2 The registration

- `:445-464` `PermissionsManagerConfig.permissionModules?: Record<string, PermissionsModule>`,
  keyed by scheme id (`"btms"`); a module "handles basket/protocol names of the form
  `p <schemeID> <rest...>`" (`:457`); "If no module exists for a given schemeID, the wallet will
  reject access" (`:462`).
- `:792-826` the constructor spreads the host's map (`:793`), then sets `nosend`, the built-in
  BRC-177 module, after it (`:795`): a host's `nosend` entry is overwritten, silently, with no
  error from the constructor (`pmodules.test.ts:117-180`, "enables and reserves the BRC-177
  nosend module": the host's module is never called). The built-in module parses the request's
  labels (`:798`; a key call's arguments carry none, so the parse answers `undefined`,
  `utility/brc177NoSendExpiry.ts:92-94`), then throws `BRC-177 noSend expiry labels are only valid
  for createAction and listActions` for every other method (`:799-801`). So a `p nosend ...`
  protocol name on a key call is refused with that text, whatever the host registered.
- There is no registration call after construction: the config is the registration.

### 1.3 The helper, `delegateToPModuleIfNeeded`

`WPM.ts:843-885` `[X]`, its arguments `(basketOrProtocolName, method, args, originator,
underlyingCall)`:

1. `:851-853` the name does not start with `'p '` (lowercase `p`, one space, case-sensitive,
   on the raw name as the caller passed it): answers `null`, the caller continues its ordinary
   flow.
2. `:855` the scheme id is `name.split(' ')[1]` (`"p btms x"` gives `btms`; `"p  x"` gives the
   empty string).
3. `:856-860` no module under that id: throws `new Error(\`Unsupported P-module scheme: p
   ${schemeID}\`)`, a plain `Error` with no WERR class (`:859`, the helper's own `throw`; `pmodules.test.ts:593-599`
   checks the text on the basket path).
4. `:862-871` the module has `handleRequest`: its answer is the call's answer; `next` calls
   `underlyingCall(transformedArgs, originator)`, and a second `next` throws `P-module p
   ${schemeID} called its underlying wallet operation more than once` (`pmodules.test.ts:1295-1380`).
5. `:874-884` otherwise `onRequest`, then `underlyingCall(transformed.args, originator)`, then
   `onResponse(result, { method, originator })`, whose answer is the call's answer.

A module's throw in any step propagates unchanged (`pmodules.test.ts:1157-1205`): the
reference wraps nothing, so the caller sees the module's own `Error` with its own text.

### 1.4 The seven key calls

Each wrapper runs the helper with `requestArgs.protocolID[1]`, its own method name, the
caller's `requestArgs`, the originator and a closure calling `this.underlying.<method>` with the
module's arguments; a non-null answer returns at once, so `ensureProtocolPermission` is never
reached for a `p ` protocol:

| Call | Wrapper | Delegation | Ordinary check after `null` |
|---|---|---|---|
| `getPublicKey` | `:5142-5188` | `:5149-5160`, only when `protocolID != null` (`:5147`) | `:5163-5171`, then the identity-key check `:5174-5185` |
| `encrypt` | `:5223-5247` | `:5226-5237` | `:5238-5245` |
| `decrypt` | `:5249-5273` | `:5252-5263` | `:5264-5271` |
| `createHmac` | `:5275-5301` | `:5280-5291` | `:5292-5299` |
| `verifyHmac` | `:5303-5329` | `:5308-5319` | `:5320-5327` |
| `createSignature` | `:5331-5357` | `:5336-5347` | `:5348-5355` |
| `verifySignature` | `:5359-5385` | `:5364-5375` | `:5376-5383` |

What follows from those lines:

- **The protocol check is skipped whole** for a `p ` name: the admin bypass, level 0, the
  admin-reserved names, the usage flags, the whitelist (#725), the cache, the seek flag (#750)
  and the prompt are `ensureProtocolPermission`'s (`:1481` on), and it is not called. The module
  is the permission.
- **The admin originator is delegated too**: the helper runs ahead of any admin test
  (`pmodules.test.ts:1243-1270`, "should still use P-module even for admin originator").
- **`getPublicKey`**: a delegated call returns before the identity-key check; with
  `protocolID` absent (the identity key alone) nothing delegates.
- **`privileged`** plays no part in the match; the module receives it inside `args`.
- **Not delegated**: `revealCounterpartyKeyLinkage` (`:5190-5203`) and
  `revealSpecificKeyLinkage` (`:5205-5221`) build their own level-2 names and never consult a
  module.

### 1.5 The other delegating calls (outside #726)

- Baskets: `listOutputs` (`:5080-5112`, delegation `:5086-5099`) and `relinquishOutput`
  (`:5114-5140`, `:5120-5131`) through the same helper on `requestArgs.basket`.
- Labels and output baskets: `createAction` (`:4294`, `:4300-4338`, the chain
  `callCreateActionWithPModules` from `:4500`), `listActions` (`:4913`, `:4920-4961`) and
  `internalizeAction` (`:5059-5078`, `:5065-5074`). These chain every module the request names,
  `onRequest` in order and `onResponse` in reverse (`:4507-4520`), and validate the P-label
  grammar (`splitLabelsByPermissionModule`, `:920-958`: `Invalid P-label format: <label>`; an
  unregistered scheme `Unsupported P-label scheme: p <id>` or `Unsupported P-basket scheme: p
  <id>`, `:890-901`).

### 1.6 The modules hosts register

- `packages/wallet/btms-permission-module/src/BasicTokenModule.ts:56` `BasicTokenModule
  implements PermissionsModule` (`onRequest` at `:196`, `onResponse` at `:233`): baskets and
  labels `p btms ...`, and `createSignature` under a `p btms` protocol (`:218`), which it
  refuses unless the digest matches a transaction it approved (`:740-825`). Its refusals are
  plain `Error`/`TypeError`.
- `packages/wallet/ecpm-permission-module/src/EcpmPermissionModule.ts:53` `EcpmPermissionModule
  implements PermissionsModule`: `handleRequest` (`:98-127`) answers `getPublicKey` under
  `p ecpm <apply|remove> <pointHex> <logicalProtocolID>` (`:160`) by deriving a key and
  multiplying a caller's point, never calling `next` (`:97`, "ECPM never forwards to ordinary
  `getPublicKey`"); every other method is refused (`:103-106`). Its `onRequest`/`onResponse`
  are identity (`:129-135`).

So the reference's own hosts use both shapes: BTMS the transform pair on a key call, ECPM the
semantic hook on a key call. A port with the pair alone serves BTMS's key-call path and not
ECPM's.

## 2. The port today

`[X]` at beta `0a036015`:

- `src/managers/permissions_manager.rs:82-106` a public trait already exists:

  ```rust
  #[async_trait]
  pub trait PermissionsModule: Send + Sync {
      async fn on_request(&self, method: &str, args: serde_json::Value, originator: &str)
          -> std::result::Result<serde_json::Value, String>;
      async fn on_response(&self, res: serde_json::Value, method: &str, originator: &str)
          -> std::result::Result<serde_json::Value, String>;
  }
  ```

  exported at `src/lib.rs:178` and `src/managers/mod.rs:72`, documented at
  `src/managers/CLAUDE.md:102`. It is the reference's `onRequest`/`onResponse` pair with JSON
  values and a `String` refusal; it has no `handleRequest`.
- `:325-327` `WalletPermissionsManagerConfig.permission_modules: HashMap<String, Arc<dyn
  PermissionsModule>>`, a public field of a public, constructible, `Default` struct; `:401-403`
  `all_enabled()` sets it empty. No built-in `nosend` entry, and nothing stops a host putting one
  in the map.
- Nothing reads the field: the only matches for `permission_modules` and `PermissionsModule`
  in `src/` are the declaration, the two constructors and the re-exports.
- `src/wallet/wallet.rs:1819-1847` the `key_permission!` macro expands to
  `check_protocol_permission(...)` (`:660-689`), which returns `Result<()>`; the seven calls
  (`:1861-1954`) each run `key_permission!(...).await?;` and then
  `self.key_route(originator).<method>(args)`. The manager is a checker the `Wallet` holds
  (`:451`, set by `set_permissions_manager` at `:637`); unlike the reference it does not wrap
  the wallet and does not implement `WalletInterface`.

**The reference's shape, weighed.** The reference's manager is itself a `WalletInterface` that
wraps the underlying wallet: each method checks or delegates, then calls `this.underlying`. The
port's manager already holds one (`WalletPermissionsManager::new` takes `underlying: Arc<dyn
WalletInterface + Send + Sync>`, `permissions_manager.rs:507-510`) but implements no
`WalletInterface` over it. A port in the reference's shape (the manager implementing the trait
over that field) would make the delegation a plain method body per call, as `:5223-5247` is, with
no macro, and would let a host stack the manager over any wallet, as the reference's hosts do. Its
cost: a public `WalletInterface` impl of 28 methods on a public type (the owner's word); a
second path to the wallet beside today's `set_permissions_manager`, whose callers would move or
keep two shapes; and the `Wallet`'s own routing (the privileged key manager behind `key_route`,
the admin originator) would have to be reached from outside through the trait. The macro inside
the `Wallet` keeps the public shape as it is. Recorded, not decided.
- `ensure_protocol_permission_seeking` (`permissions_manager.rs:772`) treats a `p btms x`
  protocol like any other: the admin bypass (`:782`), then a cached grant or a prompt.

What a host written to the reference expects, and the port does instead:

| Host does | Reference | Port today |
|---|---|---|
| Registers a module for `btms`, calls `createSignature` under `p btms x` | the module's `onRequest`, the signature over its arguments, its `onResponse` | the module is never asked; the ordinary check prompts or answers from a grant |
| Registers nothing, calls `encrypt` under `p foo x` | `Unsupported P-module scheme: p foo`, no prompt | the ordinary check prompts for `p foo x` |
| Calls `getPublicKey` under `p ecpm apply ...` with ECPM registered | the module's own key, the wallet not called | not expressible: the trait has no semantic hook |
| Admin originator under `p btms x` | the module is asked | admitted with no module |

The second row is the sharpest: a name the reference refuses unless a module owns it is a
grantable protocol in the port, so a grant an app obtains on the port under a `p ` name is one
the reference would never have issued.

## 3. What a port needs

### 3.1 The trait

The existing trait is object-safe (`#[async_trait]`, `&self`, no generics) and its two methods
are the reference's required pair. For the transform path nothing new is needed: the delegation
is a reading of a field already public. The questions are the argument shape and the semantic
hook.

**The argument shape.** The trait carries `serde_json::Value`, so the reference's `args` object
must be produced from the port's typed arguments. bsv-rs `0.9.0-beta.6`'s key argument and
result types (`src/wallet/key_args.rs:17-20`, the `key_args!` macro) derive `Debug, Clone` only,
not `Serialize`; its JSON codecs for the seven calls are `pub(super)` inside the HTTP substrate
(`src/wallet/substrates/http_json/key_requests.rs:64-189`, `JsonEncryptRequest` and its
siblings). So a lane writes seven crate-private request mappings and seven result mappings in
the toolbox (typed to the reference's JSON shape and back: `protocolID` as `[level, name]`,
`keyID`, `counterparty`, `privileged`, `privilegedReason`, `seekPermission`, byte fields as
number arrays), or asks bsv-rs for public codecs (an additive upstream PR). A module then sees
the reference's object, and a module's arguments that do not parse back are refused before the
wallet is called.

**The semantic hook.** ECPM needs `handleRequest`. In Rust it would be a third, defaulted trait
method:

```rust
async fn handle_request(
    &self,
    method: &str,
    args: serde_json::Value,
    originator: &str,
    next: PermissionsModuleNext<'_>,
) -> Option<std::result::Result<serde_json::Value, String>> { None }
```

with `None` meaning "no hook, use the pair". `next` needs a type a host can name, a public
boxed async callable (`Box<dyn FnOnce(serde_json::Value) -> BoxFuture<'_, Result<Value,
String>> + Send + '_>`, a `FnOnce` making the reference's call-once rule a type rule instead of
the `:866-867` runtime text). A defaulted method on a public trait does not break an implementor,
but the `next` alias is a new public item. Written with `futures::future::BoxFuture`, it would also
put the `futures` crate (`Cargo.toml:92`, a private dependency today) into the public API, so a
`futures` major release would become a breaking change of this crate: one more public-shape cost
for the owner. The `std` spelling avoids it: `Pin<Box<dyn Future<Output = Result<Value, String>>
+ Send + 'a>>`, the same type `BoxFuture` aliases, behind a crate-owned alias (the `next` type
itself, or a second alias beside it).

### 3.2 The public-shape answer

- **The transform pair on the seven key calls needs no new public item.** The trait, the field
  and both exports exist; the delegation, the scheme match and the JSON mappings are
  crate-private. The only public-facing change is behaviour: a registered module is asked, and an
  unregistered `p ` name is refused (section 3.4).
- **The semantic hook needs one new public item and one new trait method**: the `next` type
  (`PermissionsModuleNext` or a name the owner picks) and `PermissionsModule::handle_request`,
  defaulted. No crate-private shape suffices: a host implements the trait, so the type of an
  argument it receives must be nameable outside the crate. That is the owner's word.
- **A built-in `nosend` module** (the reference's `:795-825`) is not needed for the key calls,
  but the id is reserved. The port refuses `p nosend ` labels at both doors since #312 and #357,
  and on a key call the reference's module refuses every method except
  `createAction`/`listActions` (`:799-801`). The port refuses the same requests only if the
  delegation never reads a host's `nosend` entry: `permission_modules` is a public map
  (`permissions_manager.rs:327`), so a host can put one there, and a plain lookup would ask it
  where the reference refuses. The reservation: a host's `nosend` entry is **ignored**, as the
  reference ignores it (`:793-795`, the overwrite), and a key call under `p nosend ...` is refused
  with the reference's text, `BRC-177 noSend expiry labels are only valid for createAction and
  listActions` (`:800`), ahead of the map lookup. Ignored, not refused at construction, because
  the reference's constructor accepts the map without error and
  `WalletPermissionsManager::new` (`permissions_manager.rs:507`) is infallible: refusing would
  change a public signature or panic, where the reference does neither. With the reservation the
  refusal and its text match the reference's.

### 3.3 Where it sits in `key_permission!`

The macro today yields a check; delegation must own the call, because the module's arguments go
to the wallet and its answer comes back. One shape keeps each of the seven one line:

```rust
// encrypt, after validate_originator:
key_call!(self, originator, args, &args.protocol_id, Encrypting, encrypt)
```

expanding to: if a permissions manager is set and `args.protocol_id`'s name starts with `p `,
the crate-private `WalletPermissionsManager::delegate_key_call` (the scheme lookup, the refusal
text, the module calls), with the underlying call `self.key_route(originator).encrypt(args)` on
the module's parsed arguments; else today's `key_permission!` check, then the same
`key_route` call. The `key_route` stays the underlying: a privileged request still goes to the
privileged key manager after the module transforms it, as the reference's underlying wallet
routes it.

- `get_public_key` delegates only inside its `if let Some(ref protocol_id)` arm and returns from
  there, so the identity-key check (`wallet.rs:1874-1886`) runs only on the ordinary path, as
  `:5147-5160` returns before `:5174`.
- With no permissions manager on the `Wallet`, nothing delegates: the reference has no manager
  without one either.
- The match is on the name as passed, case-sensitive `p ` (`:851`), the scheme `split(' ')`'s
  second field (`:855`), ahead of the admin bypass (section 1.4).
- `reveal_counterparty_key_linkage` and `reveal_specific_key_linkage` keep `key_permission!`
  alone.

### 3.4 The refusal's error and class

The reference's refusals here are plain `Error`s with no WERR class: the unregistered scheme
(`:859`), the double `next` (`:867`), and whatever a module throws (BTMS, ECPM). The port's rule
for the reference's plain `Error` paths (`src/wallet/CLAUDE.md`, "The remaining direct
constructors ... carry no WERR class to preserve") is `internal_fault`: `WalletFault` of family
`Internal`, the reference's text unchanged, wire code 1 as the reference's unclassed error.
So:

| Refusal | Port error | Class at the boundary | Text |
|---|---|---|---|
| `p nosend ...` (reserved, section 3.2) | `internal_fault` | `WalletFault { family: Internal }` | `BRC-177 noSend expiry labels are only valid for createAction and listActions` |
| Unregistered scheme | `internal_fault` | `WalletFault { family: Internal }` | `Unsupported P-module scheme: p <id>` |
| A module's `Err(String)` from `on_request`/`on_response` | `internal_fault` | the same | the module's string, unchanged |
| A module's arguments that do not parse back | `ValidationError` | `InvalidParameter` | the parse's detail (port-only; the reference passes `object` through) |

**The route.** `internal_fault` is a private function of `wallet.rs` (`:2975`) returning
`bsv_rs::Error`, and the manager's functions return the crate's `Error`, whose `Internal` variant
displays `Internal error: {0}` (`src/error.rs:268`); a refusal built as `Error::Internal` in the
manager would reach the host with that prefix, not the reference's text. Of the two routes that
keep the text, the brief takes the delegation returning `bsv_rs::Error`: `delegate_key_call`
builds its refusals with `crate::error::direct_fault(WalletFaultFamily::Internal, text)`
(`src/error.rs:464`, `pub(crate)`, the function `internal_fault` wraps), and the underlying call
it runs (`self.key_route(originator).<method>(args)`, which returns `bsv_rs::Result`) passes its
own error through untouched. The other route, the delegation answering a crate-private outcome
that `wallet.rs` maps through `internal_fault`, keeps the text too, but the underlying call's
`bsv_rs::Error` would then cross the manager's crate-`Error` return and need a carrier variant or
a second mapping to come back unchanged. The class is the same `Internal` either way, and stays a
reading below.

The alternative is `AccessDenied` (`Unauthorized`, `Access denied: ` prefixed): a host could then
match a module's refusal as a permission refusal, at the cost of the reference's text and of
treating a module's internal fault as a refusal. The note records it; the class is the keeper's
reading (section 4, risk 2).

## 4. Recommendation

**Port the transform pair on the seven key calls now, under the existing public trait; hold the
semantic hook for the owner's word.**

Reasons:

1. The transform pair closes #726's finding with no public item: the trait and the field have
   been public since the manager landed and are read by nothing.
2. The refusal of an unregistered `p ` name is the larger safety gap (section 2, second row) and
   it comes with the same change.
3. BTMS, the reference's transform-pair module, signs under a `p btms` protocol; the pair serves
   it once the basket and label calls follow.
4. The semantic hook is the only piece that needs a public type, and it serves one module (ECPM)
   on one call; asking for it separately keeps the lane free of the owner's word.

Risks:

1. **The JSON shape becomes a contract.** A module reads the `Value` the port builds; seven
   hand-written mappings may drift from the reference's object (a byte field as hex instead of
   numbers, a missing `seekPermission`). The lane pins each mapping with a fixture taken from the
   reference's own JSON shape, or bsv-rs exposes its codecs.
2. **The class.** `Internal` for a module's refusal matches the reference's unclassed `Error` and
   the port's rule; a host that wants to tell a refusal from a fault cannot by class. Changing it
   later moves a host's match.
3. **Behaviour change for hosts that used `p ` names**: a host whose app was granted a `p ...`
   protocol on the port is refused after the change unless it registers a module. The CHANGELOG
   and the host note on bsv-wallet-cli#386 say so.
4. **Admin and level 0 delegate**: an admin call under a `p ` name stops being admitted outright;
   the reference does the same (`pmodules.test.ts:1243-1270`).
5. **The other five delegating calls stay undelegated** until their own issue: a `p btms` basket
   or label still takes the ordinary path, and BTMS end to end needs them.

### 4.1 The brief a lane would take

> Lane p5-726-pmodule-key-calls. Read this note, `WPM.ts:843-885` and `:5142-5385` at
> ts-stack `edf6e03`, `pmodules.test.ts:892-1293`. (1) In `permissions_manager.rs`, a
> crate-private `delegate_key_call` returning `bsv_rs::Result`: the name's `p ` match and scheme
> as `:851-855`; the scheme `nosend` reserved ahead of the lookup, refused with `:800`'s text and
> a host's `nosend` entry never read (section 3.2); the module from
> `config.permission_modules`, `Unsupported P-module scheme: p <id>` when absent, `on_request`,
> the underlying call, `on_response`; a module's `Err(String)` with its text; every refusal
> through `crate::error::direct_fault(WalletFaultFamily::Internal, ..)`, never `Error::Internal`
> (section 3.4). (2) Seven crate-private request and result JSON mappings, each
> with a fixture in the reference's JSON shape. (3) In `wallet.rs`, `key_call!` beside
> `key_permission!` so each of the seven stays one line; `get_public_key` delegates inside its
> protocol arm and returns before the identity-key check; the two linkage calls unchanged.
> (4) Witnesses: a module registered for `btms` is asked on each of the seven and its
> transformed arguments and answer are taken; an admin originator is delegated; no module for
> the scheme refuses with the text and no prompt; a non-`p` name takes the ordinary check; a
> module's `Err` crosses with its text, no `Internal error: ` prefix; a host's `nosend` module
> registered, `encrypt` under `p nosend x` refused with `:800`'s text and the host's module never
> asked; the plant (the match removed) red. (5) CHANGELOG
> under `### Changed` and the host note on bsv-wallet-cli#386. No new public item; no
> `handle_request`; no `Cargo.toml`. The fixtures suite among the gates.

The semantic hook, if the owner gives the word, is a second lane: `handle_request` defaulted on
the trait, the public `next` type, ECPM's `getPublicKey` as its witness. The basket and label
delegation (section 1.5) is a third issue, filed separately.
