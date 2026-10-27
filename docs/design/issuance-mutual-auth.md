# The issuance client's BRC-104 mutual auth, as the reference's AuthFetch does it (#315)

A decision brief for the owner, no code. Labels: `[X]` read at the pin named; `[D]` told
(the issue, the brief). Nothing here was run: every claim is a reading, with its lines.

Pins: ts-stack `edf6e03` (`~/bsv/ts-stack`); BRCs `5119a9b`; bsv-rs tag `v0.9.0-beta.1`
(the crate's pin) and `origin/beta` `6707363d` (which holds #320's `with_client`, `953d9c00`,
not in the tag); go-wallet-toolbox `e824899`; this crate at beta `02b1a87` with #269's lane
branch `origin/lane/p3-269-identity-header` at `6707b1b` (PR #314, not yet in beta), whose
`src/wallet/certificate_issuance.rs` is the text cited below as `issuance.rs`.

## 1. The reference's channel

### 1.1 The issuance door

`Wallet.ts:769-824` (`packages/wallet/wallet-toolbox/src/Wallet.ts`) `[X]`:

- `:771` the client nonce, `createNonce(this, vargs.certifier)`.
- `:772` the endpoint, ``new URL('signCertificate', `${vargs.certifierUrl}/`)``: the certifier
  URL with `signCertificate` appended.
- `:773` a fresh `AuthFetch(this, undefined, undefined, undefined, {}, createPublicHTTPSFetch(endpoint.origin))`
  per call: the wallet itself signs; no certificates requested; a new session manager; no
  originator; a fetch pinned to the endpoint's origin that refuses private addresses, a
  credential URL and any redirect (`packages/sdk/src/storage/PublicHTTPSFetch.ts:190-203,260,267-270,372-377`).
- `:774-778` the fields encrypted for the certifier and the master keyring
  (`MasterCertificate.createCertificateFields`).
- `:779-788` one `POST`, header `Content-Type: application/json` only, body
  `{clientNonce, type, fields, masterKeyring}`. The master keyring rides the authenticated
  channel inside the signed body.
- `:789-792` `response.headers.get('x-bsv-auth-identity-key')` must equal the certifier, else
  `Invalid certifier! Expected: <certifier>, Received: <value>`. There is no status check
  between `:779` and `:793`.
- `:793-795` the JSON body, `certificate` then `serverNonce` required; `:806-817` the server
  nonce, the serial HMAC, the metadata, the signature, the field decryption.

### 1.2 The handshake (BRC-103 over BRC-104)

BRC-103 `peer-to-peer/0103.md:105-123` `[X]`: A sends `initialRequest` (`identityKey`,
`initialNonce`, optional `requestedCertificates`); B answers `initialResponse` (its
`identityKey`, its own `initialNonce`, `yourNonce` echoing A's, a signature); A "verifies B's
authenticity via the signature over `(A_Nonce + B_Nonce)`, marking the session as
authenticated" (`:123`). Signatures are BRC-100 BKDS signatures (`:135-143`).

BRC-104 `peer-to-peer/0104.md` `[X]`: non-general messages are a JSON `POST /.well-known/auth`
(`:61-65,128-153`); general messages ride an ordinary request to any path with the
`x-bsv-auth-*` headers (`:71-81`); the signed request preimage is request id, method, path,
query, the whitelisted headers in lexicographic order (`authorization`, `content-type` from the
client, `x-bsv-*` but never `x-bsv-auth-*`), body (`:85-105,188-201`); the response preimage is
request id, status, the included headers, body (`:203-212`); a failed handshake or signature is
`401` or another 4xx with a JSON body (`:155-158`).

In the SDK `[X]`:

- `Peer.ts:627-664` `initiateHandshake`: a session nonce, the `initialRequest`, wait for the
  response (`:672-685`, `Timeout waiting for the BRC-103 initial response.`).
- `Peer.ts:886-955` `authenticateInitialResponse`: `yourNonce` verified as the client's own nonce
  (`:887-897`); the session found by it (`:899-902`); a pinned peer identity must match
  (`:903-908`); the signature over `sessionNonce || initialNonce` with protocol
  `[2, 'auth message signature']`, keyID `"<sessionNonce> <initialNonce>"`, counterparty the
  responder (`:910-930`); the responder's nonce claimed once (`:935-939`).
- `SimplifiedFetchTransport.ts:136-173`: the `POST <base>/.well-known/auth`, JSON,
  `redirect: 'error'` (`:121-130`); a non-2xx is `Received HTTP <status> from <url> without valid
  BSV authentication (...)` (`:143-150,464-494`).

### 1.3 The request and the response on the wire

The request (`AuthFetch.ts:644-675`, `SimplifiedFetchTransport.ts:186-218`) `[X]`: the payload is
request nonce (32 random bytes, `:252`), method, path, query (`-1` if empty), the headers
(only `content-type` with parameters cut, `authorization` and `x-bsv-*`; any `x-bsv-auth*` from
the caller throws `No BSV auth headers allowed here!`, any other header throws, `:582-601`),
body. `Peer.toPeer` signs it with keyID `"<requestNonce> <peerNonce>"`, counterparty the peer
(`Peer.ts:180-239`). The transport sends the real HTTP request to `<base><path><query>` with
`x-bsv-auth-version`, `-identity-key`, `-nonce`, `-your-nonce`, `-signature` (hex) and
`-request-id` (base64) (`SimplifiedFetchTransport.ts:189-194`), `redirect: 'error'` (`:212`).

The response (`SimplifiedFetchTransport.ts:220-367`) `[X]`: `x-bsv-auth-version`,
`-identity-key` and `-signature` required or `... without valid BSV authentication (missing
headers: ...)` (`:220-232`); the payload rebuilt as request id, status, the signed headers
(`x-bsv-*` but not `x-bsv-auth*`, and `authorization`, sorted, `:260-287`), body (`-1` if empty,
`:289-323`). `Peer.processGeneralMessage` (`Peer.ts:1270-1332`) verifies `yourNonce` as its own,
finds the session by it (`Session not found for nonce: ...`), requires the sender to match the
session, verifies the signature (keyID `"<nonce> <sessionNonce>"`, counterparty the sender), and
claims the nonce once. Only then is the sender handed to AuthFetch, which matches the request
nonce (`AuthFetch.ts:437-439`), records the peer (`:441-445`), rebuilds a `Response` and sets
`x-bsv-auth-identity-key` to the verified sender (`:479`), overwriting anything the server
wrote. A 30 s deadline covers it (`:90,298-300`, `Timed out waiting for authenticated response.`).

### 1.4 What the certifier verifies

`auth-express-middleware/src/index.ts` `[X]`: `/.well-known/auth` takes the handshake
(`:1095-1096`); a request with `x-bsv-auth-request-id` is a general message (`:1097-1098`); a
request with any other `x-bsv-auth-*` header and no request id is refused as
`Partial authentication headers are not accepted.` (`:1099-1102`), answered `400`
`{status:'error', code:'ERR_AUTH_MALFORMED', description:'The authentication request is malformed.'}`
(`:882-892`); a request with no auth header is `401`
`{status:'error', code:'UNAUTHORIZED', message:'Mutual-authentication failed!'}` unless
`allowUnauthenticated` (`:1778-1793`). The handshake answer sets `x-bsv-auth-version`,
`-message-type`, `-identity-key`, `-nonce`, `-your-nonce`, `-signature` and sends the
`initialResponse` JSON (`:1005-1031`); the general answer is status, the handler's headers plus
`x-bsv-auth-version`, `-identity-key` (the certifier's), `-nonce`, `-your-nonce`, `-signature`,
`-request-id` (`:931-968`). The certifier's handler sees the client's proven identity key, so
the subject it certifies is a key the requester proved.

### 1.5 The branches the issuance call cannot take

- The fallback (`AuthFetch.ts:680-705`) refuses any `x-bsv*` response header
  (`:693-696`). It is reached when `supportsMutualAuth === false` (`:246`), which only this
  function sets (`:700`), or from `recoverAuthenticatedSend` on an error text
  `HTTP server failed to authenticate` (`:422`). At `edf6e03` no non-test source in
  `packages/sdk/src` throws that text (`git grep` finds `:422` and four test mocks only), so on
  the live path a non-BRC-104 certifier's handshake error is rethrown (`:425`). Were the fallback
  taken, `Wallet.ts:789` would require the header the fallback forbids. Either way the reference
  issuance passes only a peer that signed.
- The stale-session retry (`:400-421`) needs a known peer identity (`:405`); a fresh AuthFetch
  per call (`Wallet.ts:773`) has none before its response.
- The 402 branch (`AuthFetch.ts:334-337,711-845`): an authenticated `402` from a certifier is
  paid through the wallet's `createAction` and retried. Live, but no certifier shape we know asks.
- A certifier's `certificateRequest` is answered from the wallet's certificates
  (`AuthFetch.ts:376-396`); the issuance AuthFetch requests none itself (`Wallet.ts:773`).

### 1.6 Go

go-wallet-toolbox `e824899` `[X]`: `pkg/wallet/wallet.go:715-720` posts to `<certifierUrl>/signCertificate`
through `w.auth.Fetch` (go-sdk `authhttp.AuthFetch`, built at `:262`); `wallet_acquire_certificate_issuance.go:148-150`
compares `x-bsv-auth-identity-key` with the certifier. Same channel as the reference.

## 2. The port today

`issuance.rs:529-590` `send_issuance_request` `[X]`: one `POST` to `certifier_url` as given
(`:542`; the guard strips a trailing `/`, `:319,373`; no `signCertificate` is appended, and the
fixtures post to `/issue`), through #182's policy client (`:539`), with three headers:
`x-bsv-auth-version: 0.1`, `x-bsv-identity-key: <wallet key>`, `Content-Type` (`:543-545`).
A non-2xx is `Certifier returned HTTP <status>: <body>` (`:553-558`). Then #269's check: the
`x-bsv-auth-identity-key` value, joined as fetch joins it, must equal the certifier, else
`Invalid certifier! Expected: ..., Received: <value|null>` (`:561-576`); then the JSON body.

| | the reference | the port |
|---|---|---|
| the certifier proves its key | yes: two signatures (handshake, response) verified against it | no: a written header is compared |
| the client proves its key | yes: the request is signed; the certifier's `req.auth.identityKey` | no: an unsigned `x-bsv-identity-key` header |
| the request and response are bound to the session | yes: signed both ways | TLS only |
| path | `<certifierUrl>/signCertificate` | `<certifierUrl>` |
| a reference-middleware certifier | answers | answers `400 ERR_AUTH_MALFORMED` (`index.ts:1099-1102`: the port's `x-bsv-auth-version` with no request id) |
| redirects | refused | refused by the policy client under test; reqwest's default in production `[X]` (`http_client.rs:9`) |
| non-2xx | read as a body (authenticated) | refused before the header |

What the header check proves: that whoever answered wrote the certifier's public key, which is
public. It proves nothing a server without the certifier's private key cannot write. The later
checks carry the cryptography (the fields and keyring are encrypted to the certifier,
`Wallet.ts:774-778`; the serial HMAC at `:807-814` and the signature at `:816` need its key), so
an impostor reads no field and forges no certificate; what the port lacks is the channel's two
proofs: the certifier is told, not shown, which key asks, and the client is told, not shown,
who answered. And a reference certifier refuses the port's request outright (the table), so the
port's issuance today reaches only a certifier that reads the unsigned header.

## 3. The design

### 3.1 The client

One crate-private function replaces `send_issuance_request`, in a new
`src/wallet/certificate_issuance_auth.rs` (under 300 lines):

1. The base URL is the endpoint's origin; the request path is the endpoint's path. Whether the
   endpoint gains `/signCertificate` (`Wallet.ts:772`) is its own behaviour change and its own
   issue (finding F4), landed before or after this one, never inside it.
2. A Peer per call, as `Wallet.ts:773` builds an AuthFetch per call:
   `Peer::new(PeerOptions { wallet: <the wallet's ProtoWallet clone>, transport,
   certificates_to_request: None, session_manager: None, auto_persist_last_session: false,
   originator: None })` (bsv-rs `src/auth/peer.rs:103-115,175`), wrapped in `Arc`.
3. The verifying callback. bsv-rs's client callback `Peer::start` verifies nothing: an
   `initialResponse` is marked `is_authenticated = true` with no signature check
   (`peer.rs:226-284` at beta, `:258,269` at the tag), and a general message goes to the
   listeners with no session or signature check (`peer.rs:298-307`). The verifying path is
   `handle_incoming_message` (`peer.rs:605-625`): `process_initial_response_inner` verifies the
   handshake signature (`:836-855`), `process_general_message` finds the session by the sender
   and verifies the signature (`:986-1004`). `start_server` installs it (`peer.rs:349-355`) and
   needs only `Arc<Peer>`. The issuance client uses `start_server` until bsv-rs's `start`
   verifies (finding F1); with `start_server` a bad signature fails `to_peer` itself, since the
   transport returns the callback's error (`http.rs:601-613,842`).
4. The request: `HttpRequest { request_id: <32 random bytes>, method: "POST", path, search: "",
   headers: [("content-type", "application/json")], body }` (`http.rs:76-98,223`), sent with
   `to_peer(&payload, Some(certifier_hex), Some(30_000))` (`peer.rs:365-397`). The two
   home-grown headers and their constants (`issuance.rs:63-70`) go: the transport writes the
   `x-bsv-auth-*` set (`http.rs:534-571`).
5. The response: a general-message listener (`peer.rs:491-516`) takes the payload whose leading
   32 bytes are the request id, as the storage client's does (`storage_client.rs:524-546`), and
   keeps the sender. Then, in the reference's order: the sender, which `process_general_message`
   has bound to a signed session, must equal the certifier, else
   `Invalid certifier! Expected: <certifier>, Received: <sender hex>` (`Wallet.ts:789-791`);
   `HttpResponse::from_payload` (`http.rs:324`); the body to JSON, `certificate`, `serverNonce`
   (`Wallet.ts:793-795`). No status gate, as `Wallet.ts:779-793` has none: a signed 4xx body
   reaches `No certificate received from certifier!`. The response deadline is 30 s with
   `Timed out waiting for authenticated response.` (`AuthFetch.ts:298-300`).
6. Faults keep bsv-rs's class: `transport_fault()` maps to `Error::SdkError`, the rest to
   `Error::NetworkError`, as `storage_client.rs:566-580` does. No new variant.
7. Not ported, each a stated limit with its issue: the 402 payment (F5), the certifier's
   certificate request (F6), the private-address refusal of `createPublicHTTPSFetch` (F7).

Why a ProtoWallet and not the `Wallet`: the reference passes the wallet (`Wallet.ts:773`), whose
`createSignature`/`verifySignature`/`createHmac` are the proto's after an originator check. The
port's `Wallet::create_signature` adds a BRC-98/99 protocol permission check
(`src/wallet/wallet.rs:1846-1856`) the reference's Wallet does not make, and `Peer` needs an owned
`W: 'static` (`peer.rs:170`), which `&Wallet<S, V>` is not. The `ProtoWallet` holds the same
root key, is `Clone` and a `WalletInterface` (bsv-rs `src/wallet/proto_wallet.rs:148,617` at the
tag), so the handshake signs with the same identity and no generic bound moves.

### 3.2 The transport under #182's policy

Production: `SimplifiedFetchTransport::with_client(origin, client)` (bsv-rs beta
`src/auth/transports/http/client.rs:58-64`) with a reqwest client built as
`client::default_client` is (30 s, 10 s connect, no idle reuse, `Policy::none()`, `:30-38`), or
`SimplifiedFetchTransport::new(origin)`, which on beta builds exactly that (`http.rs:513-520`).
Either way a redirect is refused on both paths (`client.rs:67-81`, `http.rs:645,745`), as the
reference's `redirect: 'error'`. At the tag `new` builds a client without the redirect policy
(`v0.9.0-beta.1 http.rs:519-524`), so reqwest follows up to ten hops and a signed request can be
replayed to the hop's host: that is what beta.2 buys.

Test and bench-support: the storage client's guarded wrapper (`storage_client.rs:48-85`), which
checks the base URL on every send including the handshake, moves to a shared crate-private
module (for example `src/services/providers/http_client.rs`) and both clients use it. Its
`check_url_str` is gated on `feature = "remote"` (`http_client.rs:13`); issuance builds without
`remote`, so the gate widens to `any(test, feature = "bench-support")`. With beta.2 the wrapper
also hands the SDK transport the isolated client's inner reqwest client (no proxy, localhost
pinned, `http_client.rs:66-90`) through `with_client`, so the policy covers the connection, not
only the URL. `ISSUANCE_TEST_URL` (`issuance.rs:52-57,536`) keeps routing a validated public URL
to the loopback fixture.

### 3.3 The fixtures

A mockito certifier that performs the handshake, in a new `certificate_issuance_peer_fixture.rs`
(test-only, under 400 lines):

- `POST /.well-known/auth` with `with_body_from_request` and `with_header_from_request`
  (mockito 1.7, `src/mock.rs:435,524`): parse the `initialRequest`, answer the
  `initialResponse` JSON with the certifier's key, a nonce derived from the request (so the two
  closures agree), `yourNonce` echoed, and the signature over `yourNonce || initialNonce`, keyID
  `"<yourNonce> <initialNonce>"`, as `index.ts:1005-1031` sends it.
- `POST <path>`: read the `x-bsv-auth-*` headers, verify the client's signature over the
  rebuilt request payload, answer the body the existing fixtures build, with
  `x-bsv-auth-version`, `-identity-key`, `-nonce`, `-your-nonce`, `-request-id` and
  `-signature` over request id, status, the signed headers, body (`index.ts:931-968`).
- The closures are synchronous; sign with bsv-rs's key deriver and `PrivateKey::sign` rather than
  the async `ProtoWallet`, and one unit test checks the fixture's signature verifies through
  `ProtoWallet::verify_signature` (planted: flip a byte, the test fails).

The storage client's fixtures cannot be reused: they run with `use_auth` off
(`storage_client_network_isolation_tests.rs:13`) or test refusals and faults
(`transport_fault_tests.rs:14`); none answers a handshake.

The witnesses, each red on today's code and green after:

| case | reference | the expected outcome |
|---|---|---|
| a signing certifier | `Wallet.ts:779-824` | the certificate is stored |
| a signing server with another key | `Wallet.ts:789-791` | `Invalid certifier! Expected: C, Received: <its key>` |
| a plain server writing `x-bsv-auth-identity-key: C` | `SimplifiedFetchTransport.ts:220-232`; `Peer.ts:1282-1305` | refused before the body; no keyring sent past the handshake |
| a bad response signature | `Peer.ts:1303-1305` | refused, body unread |
| a handshake answered `404` | `SimplifiedFetchTransport.ts:143-150` | refused, no general request sent (`expect(0)`) |
| the request carries no caller `x-bsv-*` header | `AuthFetch.ts:582-601` | the fixture's match on headers |
| a signed `400` with a JSON error body | `Wallet.ts:779-795` | `No certificate received from certifier!` |
| a redirect on either path | `SimplifiedFetchTransport.ts:128,212` | refused (beta.2) |
| a non-loopback URL under test | #182, #248 | refused before transport (the current isolation witness, kept) |

The #269 header witnesses (`certificate_issuance_header_tests.rs`) describe a header the client
no longer reads; they are rewritten in the behaviour commit, the `docs/ARCHITECTURE.md` rows at
`:148-152` of #269's branch with them.

### 3.4 The public shapes

Moves: `Cargo.toml`'s bsv-rs `=0.9.0-beta.1` to `=0.9.0-beta.2`, for #320's redirect refusal and
`with_client`; the owner's word, the pin lane's change, with the CLI's pin of the same version.

Does not move (each checked):

- `acquire_certificate_issuance` and `send_issuance_request` live in a private module
  (`src/wallet/mod.rs:92`, `mod certificate_issuance;`, no re-export); their arguments change
  freely. `ProtocolIssuanceRequest`, `ProtocolIssuanceResponse` and `CertificateResponse` are
  `pub` inside it and not exported.
- `Wallet<S, V>` gains no bound: the ProtoWallet clone avoids `W: Clone + 'static`. If the owner
  wants the port's BRC-98/99 check on the handshake signatures, the `Wallet` must be the Peer's
  wallet and that bound is the shape that moves.
- `Error` gains no variant.
- `StorageClient`'s public surface is unchanged by the wrapper's move (the type is named in
  `StorageClient::peer`'s return, `storage_client.rs:423`, and keeps its name and path through a
  `use`).

The wire moves, which is the point: the request becomes a handshake plus a signed general
request, and the texts in 3.5 change. A behaviour change with its fixture in its own commit.

### 3.5 What an operator sees

A certifier behind the reference middleware sees, per issuance: a `POST /.well-known/auth` from
the wallet's identity key, then `POST <path>` with the six `x-bsv-auth-*` headers, its handler
called with `req.auth.identityKey` = the wallet's key. Today it sees one request it answers `400
ERR_AUTH_MALFORMED`. A certifier that read `x-bsv-identity-key` stops receiving it.

The wallet's caller sees:

| situation | today | after |
|---|---|---|
| certifier answers the handshake with another key | (no handshake) | `Invalid certifier! Expected: C, Received: K` |
| plain HTTPS certifier | `Invalid certifier! ... Received: null` or its body | `Network error: Failed to send request: ...` with bsv-rs's `/.well-known/auth` status text (`http.rs:646-647`) |
| reference certifier | `Network error: Certifier returned HTTP 400 Bad Request: {...ERR_AUTH_MALFORMED...}` | the certificate |
| unsigned response to the signed request | n/a | the bsv-rs refusal (`No session with sender` today, F3 below asks for the reference's `without valid BSV authentication`) |
| no answer within 30 s | reqwest's timeout text | `Timed out waiting for authenticated response.` |

## 4. The options

### (a) BRC-104 on 0.14, as the pin lane's follow-on once bsv-rs beta.2 is in

Cost: the pin lane (owner's word) lands beta.2; then one lane, about four commits: the shared
guarded transport (a refactor, no behaviour change), the fixture certifier with its
self-check, the behaviour commit (client, witnesses, the #269 witnesses rewritten, the
ARCHITECTURE rows, CHANGELOG under `## [Unreleased]`), docs; an estimate of under 1000 added
lines, no file past 800. Dependencies outside the crate: none blocking (the `start_server` path
verifies at both refs); F1 and F2 are bsv-rs issues that make the code plainer, not preconditions. Risk: the
response preimage bsv-rs rebuilds (`http.rs:769-790`) has never been checked against the
middleware's signature by this crate, since the storage client never verifies
(`storage_client.rs:355`, its listener ignores `_sender`, `:526`); the exit test (5) is where a
bsv-rs bug would show, and it would go to beta.3.

A variant, (a') on the tag now: everything above works on `v0.9.0-beta.1` with
`SimplifiedFetchTransport::new` except the redirect refusal, which the tag's client lacks.
Not recommended: a followed redirect replays the signed keyring to another host, which is the
leak the change exists to close.

### (b) Plain until 0.15, the header check as the stated limit

Cost now: one paragraph in `docs/ARCHITECTURE.md` and a row in its divergences. But the limit to
state is wider than "the header is unauthenticated": the port's request is refused by the
reference middleware before any header is read (`index.ts:1099-1102`), so (b) states that the
port's issuance reaches no reference certifier until 0.15, and that the certifier cannot prove
which key asked. Holding it costs the #269 witnesses' upkeep and leaves `x-bsv-identity-key`,
a header no reference reads, on the wire.

### The keeper's reading and the recommendation

The keeper's reading on the issue `[D]`: a proven peer, not a written header. Only (a) meets it.

Recommendation: (a), the captain's reading. Reason: the reference's issuance passes only a peer
that signed (1.5), the port's plain request does not reach a reference certifier at all (2), and
beta.2 carries the redirect refusal without which the authenticated request would be the one
that leaks; the toolbox's public surface does not move, only the pin the pin lane already moves.

## 5. The exit test

The in-crate fixture (3.3) proves the client against this crate's reading of the middleware.
The interop proof replays shapes recorded from the reference middleware itself:

1. A recording script (Node, loopback only, kept under
   `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/`) runs `auth-express-middleware` at `edf6e03` with
   a `ProtoWallet` certifier on the test vectors' known key (`test_vectors/keys/`) and a
   `signCertificate` handler answering `{certificate, serverNonce}`, drives it with the
   reference's `AuthFetch`, and writes each exchange (the `initialRequest`/`initialResponse`
   JSON, the general request and response headers and bytes, the two session nonces) to
   `test_vectors/issuance/`.
2. A Rust vector test, offline: from each recorded response, bsv-rs rebuilds the payload and
   `verify_signature` accepts the recorded signature with the recorded nonces; one byte of the
   body flipped and it refuses (planted, shown failing, reverted).
3. The shapes recorded: (i) the handshake `200` `initialResponse`; (ii) a signed `200` issuance
   body; (iii) a signed `400` with an error JSON from the handler; (iv) the `401 UNAUTHORIZED`
   answer to an unsigned request; (v) the `400 ERR_AUTH_MALFORMED` answer to today's port
   request; (vi) an empty-body signed response, since the reference writes `-1` for an empty
   body (`SimplifiedFetchTransport.ts:316-321`) and the rebuild must match it.
4. Once, live on loopback, the Rust client against the recording script's server: the
   certificate stored, the middleware's log naming the wallet's key as `req.auth.identityKey`.
   The run's log goes to the lane's report and under the out volume.

Done when 2, 3 and the table in 3.3 are green in CI and 4 is in the report.

## Findings to file

- F1 (bsv-rs): `Peer::start`, the client callback, verifies neither the `initialResponse`
  signature (`peer.rs:226-284`) nor a general message's session and signature (`:298-307`), where
  `Peer.ts:886-930,1270-1305` verify both. The storage client uses `start`
  (`storage_client.rs:345`) with `verify_responses: false` (`:355`): its responses are no more
  proven than issuance's header. One issue in bsv-rs, one in this crate for the storage client.
- F2 (bsv-rs): `initiate_handshake` drops the identity it is given (`_identity_key`,
  `peer.rs:704`), where `Peer.ts:903-908` refuses a responder that is not the pinned peer; and
  `process_initial_response_inner` continues on a failed nonce check (`peer.rs:857-874`) where
  `Peer.ts:887-897` throws. `process_general_message` does not check `yourNonce` or claim the
  nonce (`peer.rs:986-1013`; `Peer.ts:1271-1314`).
- F3 (bsv-rs): an unsigned general response becomes a message carrying the client's own
  identity (`http.rs:755-802`) rather than the reference's
  `without valid BSV authentication (missing headers: ...)` (`SimplifiedFetchTransport.ts:220-232`).
- F4 (toolbox): the issuance endpoint omits `signCertificate` (`issuance.rs:542`; `Wallet.ts:772`;
  Go `wallet.go:715`).
- F5 (toolbox, owner's word): an authenticated `402` from a certifier is paid by the reference
  (`AuthFetch.ts:334-337`); the design refuses it as a stated limit.
- F6 (toolbox): a certifier's certificate request is answered by the reference
  (`AuthFetch.ts:376-396`); the design answers none.
- F7 (toolbox): `createPublicHTTPSFetch`'s refusal of private addresses at connect
  (`PublicHTTPSFetch.ts:260-270`) has only a hostname-shape guard in the port
  (`issuance.rs:338-374`).

Not read: the go-sdk's `authhttp.AuthFetch` itself (only the toolbox's call); a certifier
service's source (none in ts-stack at `edf6e03`; the reference's own issuance test mocks
`AuthFetch.prototype.fetch`, `Wallet.acquireCertificate.test.ts:412-417`).
