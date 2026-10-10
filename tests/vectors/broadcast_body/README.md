# The broadcast-body rows

Six rows of the broadcast-body replay of bsv-stack-lean #63 (`corpus/runners/broadcast-body/`, the rows from
`corpus/beef-of-any-size/bytes/`), the bytes the poster-side test `tests/arc_broadcast_body_tests.rs` hands
`Arc::post_beef`. Three AtomicBEEFs (BRC-95: `01010101` and the subject's txid, then the BEEF) and the same three
BEEFs without the prefix. `example` is the BRC-62 example; `small_chain_true` is a chain of three unproven
transactions locked by `OP_TRUE`; `lone` is one unproven transaction.

| row | bytes | sha256 | ARC e7efc5b's intake (the replay) |
|---|---|---|---|
| `example_atomic_payment.bin` | 713 | `b6b08e4c...a224c9` | read as raw: 400 |
| `example.bin` | 677 | `530b9a60...87a4d8` | BEEF, parsed |
| `small_chain_atomic_true.bin` | 344 | `2d7b8470...4e8c48` | read as raw: 400 |
| `small_chain_true.bin` | 308 | `6b5eb223...c5ea4f` | BEEF, parsed |
| `lone_atomic.bin` | 145 | `4fc97f61...8ce3bc` | read as raw: 400 |
| `lone.bin` | 109 | `38656f4c...1fbb50` | BEEF, parsed |

The full digests are in the test (`ROW_SHA256`), which checks them. Never edited by hand.
