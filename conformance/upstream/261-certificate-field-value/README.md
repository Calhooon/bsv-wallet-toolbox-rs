# Certificate field encryption witness (#203, bsv-rs #261)

`certificate_field_value_261.json` is copied byte for byte from
`tests/vectors/certificate_field_value_261.json` at bsv-rs commit
`2e4b477ffbf7866cc26e570d304bf08d2dd8aa8a` (beta).

SHA-256: `b2ed75f0cf7ee53acf9b419353e4ab3b4177886aacb612a29b2e0a17a779c5ea`.

The producer is `tests/support/certificate_field_value_261.mjs` at that
same commit. It loads the reference SDK from `ts-stack@edf6e03`, replaces
its randomness with a SHA-256 counter stream, and records certificates,
keyrings, signed bytes, signature verdicts and decrypted plaintexts in
both directions. The producer's header contains its reproduction commands.
Do not edit the fixture to change a test result.

The witnesses live in `src/wallet/certificate_issuance_tests.rs` and run with:

```sh
env HTTP_PROXY=http://127.0.0.1:9 HTTPS_PROXY=http://127.0.0.1:9 ALL_PROXY=http://127.0.0.1:9 NO_PROXY=127.0.0.1,localhost,::1 no_proxy=127.0.0.1,localhost,::1 cargo test --lib certificate_issuance::field_scheme_tests
```

The decrypt procedure is ported from the pinned reference's
`MasterCertificate.decryptField` and `VerifiableCertificate.decryptFields`:
wallet-decrypt the field key, Base64-decode the field, AES-decrypt it with
that key, then decode UTF-8. This crate pins bsv-rs 0.9.0-beta.1, whose
`VerifiableCertificate::decrypt_fields` instead returns the wallet-decrypted
entry as plaintext, so that method cannot serve as this witness's oracle.

Signature verification uses the reference's `Certificate.verify` rule
(`Certificate.ts:425-435`): derive the certifier's signature key under
`[2, "certificate signature"]`, key ID `"<type> <serialNumber>"`, and the
anyone counterparty, then verify the signed bytes. The pinned bsv-rs
`Certificate::verify` uses the raw identity key; its #251 correction belongs
to the separate toolbox #199 pin lane.
