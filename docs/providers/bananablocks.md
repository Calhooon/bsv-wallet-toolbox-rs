# BananaBlocks: probe notes

GorillaPool's explorer, the third rung of the services' read ladders beside
WhatsOnChain and Bitails (0.12.0, bsv-wallet-cli#50). The provider is
`src/services/providers/bananablocks.rs`. These notes are what the probes of
2026-10-22 saw (the first lane's notes, the review of PR #43, and the lane
that folded the review). They were read-only GETs, paced.

## Hosts

| Chain | Base |
|-------|------|
| Mainnet | `https://bananablocks.com/api/v1` |
| Testnet | `https://test.bananablocks.com/api/v1` |

No key. `ServicesOptions::with_bananablocks_base(url)` moves the base,
`without_bananablocks()` (or an empty base) turns the provider off.

## Rate limit

Every answer carries `x-ratelimit-limit: 80` and `x-ratelimit-remaining`.
The host's budget is 80 requests a minute. This crate paces itself per
process with one token bucket: a burst of 30 refilled at 0.5/s, so 60 in
the first minute and 30 a minute sustained, under BananaBlocks' 80. The
bucket is shared by every provider the process builds for one base
(`RateGate::shared`). The
daemon builds two `Services` per tenant, so a gate per `Services` would
double the rate. A 429 closes the rung for its `Retry-After` (60 s when none
is sent). Inside that window a call is `Error::RateLimited` and makes no
request. Nothing sleeps on a 429.

## Endpoints

### `GET /tx/{txid}/hex`: the raw transaction

200: the transaction as text hex. The bytes must hash to the txid; other
bytes are a fault. 404 means no such transaction.

### `GET /tx/{txid}`: the transaction and its block

200: a JSON document with `txid` (must be the asked txid), `block_hash`,
`block_height` (both present once mined, absent or empty before),
`block_time`, `block_index`, `confirmations`, `fee`, `inputs[]` and
`outputs[]` (each output as `/txo` gives it). An observed body (block
972181, trimmed):

```json
{"txid":"27e82546…db1c","hash":"27e82546…db1c","version":1,"locktime":0,"size":522,
 "block_hash":"00000000000000000c98795f0b660963a91d3fc116d4865f1764e0cc55f5093d",
 "block_height":972181,"block_time":1792685005,"block_index":99,"fee":55,
 "confirmations":1,"inputs":[…],"outputs":[…]}
```

404 (`{"error":"Transaction not found"}`) means this index does not know the
transaction. That is inconclusive: the status ladder treats it as a gap,
never as "absent".

### `GET /txo/{txid}/{vout}`: one output and its spend

200, unspent (observed 2026-10-22):

```json
{"value":1945,"index":1,"script_pubkey":"76a914e0de61b233403b703ef20fb6b0f5dc0389b5c48a88ac",
 "script_type":"p2pkh","address":"1MVzmKLnHNJ9Dkv7xb8iQUzy2mKrJNzTcH","spent":false}
```

200, spent (block 170's coinbase-era output):

```json
{"value":1000000000,"index":0,"script_pubkey":"4104ae1a…ac","script_type":"p2pk",
 "address":"1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3",
 "spent_txid":"ea44e97271691990157559d0bdd9959e02790c34db6c006d779e82fa5aee708e",
 "spent":true,"spend_scope":"confirmed"}
```

404: `{"error":"TXO not found"}` (a vout past the end) or
`{"error":"Transaction not found"}`.

The review of #43 reported a `script_hash` field (the big-endian sha256 of
the script). The lane's probes did not see one on any of four outputs; they
saw `script_pubkey`. The parser takes the output's script hash as the
big-endian sha256 of `script_pubkey`. If a `script_hash` is present it must
equal that hash, and it stands alone only when no `script_pubkey` is given.
A body with neither is a fault.

How the ladder reads it (`get_utxo_status`):

- `spent: false` counts only when the output's script hash equals the asked
  hash: `hashLE` / none reversed, `hashBE` as given, `script` hashed with
  sha256 and reversed. Any other script is a fault, including the empty
  script `is_utxo` asks with when none is stored. An output whose script is
  not held therefore never comes back unspent from here.
- `spent: true` needs a 64-hex `spent_txid` that is not the outpoint's own
  transaction. `spend_evidence` binds that name by the spender's own bytes.
  When the ladder already read `/txo` for the outpoint, `spend_evidence`
  takes the `spent_txid` from that answer instead of asking again, so an
  unspent verdict costs one BananaBlocks request when it answers; after a
  fault `spend_evidence` asks it once more (up to three with a 5xx retried
  in the ladder).
- A 404 (`TXO not found`), a malformed body or a script mismatch is a
  deterministic fault: the ladder's retry round does not ask again. Only a
  5xx, a timeout or a transport failure is retried, once, after 2 s.
- `/txo/{txid}/{vout}/spend` (`{"spent":false}` or
  `{"spent":true,"spentTxid":…}`) also answers, but carries no script. The
  crate no longer asks it.

### `GET /tx/{txid}/proof`: the TSC proof

200 for a mined transaction:

```json
{"index":0,"txOrId":"<txid>","targetType":"merkleRoot","target":"<merkle root>","nodes":["<hash>","*",…]}
```

The target is the block's merkle root, not its hash. The provider folds the
nodes to that root, then hands the proof on with the block hash from
`/tx/{txid}` as the target. The ladder then checks the root against the
header service, as it does for every courier's proof. The probe's proof for
block 972169 folded to the root that two Chaintracks deployments hold.
`/tx/{txid}/proof/tsc` is no route.

### `GET /address/{address}/utxos`: an address's unspent outputs (leads)

Probed 2026-10-22 (read-only, paced). BananaBlocks lists unspent outputs by
**address only**. No route takes a script hash: `/script/{hash}`,
`/script/{hash}/utxos` (big- and little-endian), `/script/{hash}/unspent`,
`/scripthash/{hash}/utxos`, `/script_hash/{hash}/utxos`,
`/scripts/{hash}/utxos` and `/hash/{hash}/utxos` are all `404 page not
found`, as are `/address/{address}/unspent`, `/address/{address}/history`,
`/openapi.json`, `/swagger.json` and `/docs`.

200 (trimmed; newest first, a page of 20 unless `?limit=` asks for more; the
host served `limit=1000`):

```json
{"items":[{"txid":"b4ebcb258d4ac9b7811ae032ec66a68437a6f370b14bccd8b29b3f4665ab0600","vout":1,
  "value":1945,"script_type":"p2pkh","block_height":972200,"block_time":1792697684}, …],
 "cursor":"OTcyMjAwOjE6…","hasMore":true}
```

`?cursor=` takes the next page. An address never seen answers
`{"items":[],"hasMore":false}`. `GET /address/{address}` gives a summary
(`balance`, `utxo_count`, `tx_count`, and a `freshness` block with the
`indexed_height`).

How the crate reads it (`BananaBlocks::list_address_unspent`, behind
`Services::get_utxo_leads`): the first page with `limit=1000`, as LEADS, never
a verdict. Only a 200 whose `items` each carry a 64-hex `txid`, a `vout` and a
`value` answers; a 404, a 429, a 5xx or another shape is a fault, never an
empty list. A page with `hasMore` is still an answer (its leads; logged).
Because no address is derived from a hash, BananaBlocks is a lead source only
when the caller hands the script itself (`GetUtxoStatusOutputFormat::Script`)
and the script is P2PKH; asked by script hash, it is not asked.

Not seen: an unconfirmed output still unspent in this list. In the probe an
address paid by a WhatsOnChain-mempool transaction did not list that output,
but `/txo` showed it already spent in the mempool (`spend_scope:
"unconfirmed"`), so its absence was right; whether the list carries mempool
outputs is unconfirmed. A lead missing here may be in WhatsOnChain's or
Bitails' list, which is why every explorer is asked and the union kept.

## The mempool gap

BananaBlocks' index misses some mempool spends. On 2026-10-22 transaction
`659e6e1d…2f85aca` was in WhatsOnChain's mempool spending
`826718bf…ac9dd1:0`. WhatsOnChain's `/tx/{txid}/{vout}/spent` named it, but
BananaBlocks answered `200 {"spent":false}`, and still did on a re-check.
In a 12-transaction sample of WhatsOnChain's mempool, BananaBlocks knew all
12, so this is a gap in its index, not a blanket lag. Whether it closes is
unknown.

The consequences in the code:

- In `get_utxo_status`, BananaBlocks' unspent, like every explorer's,
  stands only with a second explorer's clean unspent (the owner's rule: a
  negative, "unspent" or "absent", needs a second provider).
- `is_utxo` asks for a spender even after two unspent answers. A spender
  bound by its own bytes stands on one provider.
- In `get_status_for_txids`, BananaBlocks is not a chain index. Its 404
  (`unknown`) is a gap, never an absence vote: it is left out, and the
  answer says "could not look". Its `known` holds against one chain
  index's `unknown`; a transaction's absence stands only when WhatsOnChain
  and Bitails both answer `unknown`, and then it stands against any
  `known`, BananaBlocks' or a broadcaster's.
