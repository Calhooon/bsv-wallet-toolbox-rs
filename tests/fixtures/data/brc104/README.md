# BRC-104 bounds at the beta.3 pin

The six JSON files contain 55 cases extracted from bsv-rs tag
`v0.9.0-beta.3` (`bf97cc74`),
`tests/vectors/auth_transport_bounds_416.json`, its `ts-stack@edf6e03`
reference. Each retained case is unchanged; only the outer grouping differs.
The source file's SHA-256 is
`a937c24497cda2546cdf71700a77b9bda356cec92e5bf5cdb2635f50b7774856`.
Its recorder is `tests/support/auth_transport_bounds_416.mjs` at that tag.

The toolbox replay uses mockito over HTTP/2. Four Latin-1 cases are omitted:
mockito's string header writer encodes UTF-8, so it cannot reproduce their
single-byte wire characters. Two artificial Content-Length cases need a
constructed fetch response rather than native HTTP framing and are omitted.
Five configurable-limit cases are outside this replay of the six default
bounds. Those eleven cases remain in the upstream recording.

The signature family includes `MAA=`, the certificate header and body families
include the exact limits and one byte over, and the signed-header family
includes each name, value, count and total limit. Message-type and request-ID
cases pin the callback payload as well as the refusal text. These are
transport deliveries, before a Peer's signature or certificate policy.
