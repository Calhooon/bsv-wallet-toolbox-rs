# The path files' survivors under the widened oracle, 2026-11-02

Base `42559923`, the base of the complete record [mutants-complete-2026-11-02.md](mutants-complete-2026-11-02.md). That record scores every file under the `--lib` oracle; for the eight files on the paths that judge the node's word and the chain's evidence that score below 80 there, an integration target also enters the file, so the `--lib` figure is a lower bound. This record runs each of their 217 `--lib` survivors once more under the widened oracle and gives each file's widened figure beside its `--lib` figure. Every value is written by `widened.py` in the ledger folder from its runs; the machine-readable figures, the command and every survivor's verdict, host and catching test are [mutants-paths-widened-2026-11-02.json](mutants-paths-widened-2026-11-02.json). The floors in `tools/mutants-floor.json` stand as the floors lane wrote them; their comments carry these figures.

## Method

`cargo-mutants 27.1.0`, one survivor per `--in-place --baseline skip` invocation after one green filtered baseline per worker (358 passed, 0 failed over the lib and 12 integration targets). The oracle: the --lib oracle's 1733 default-features lib tests are the record's verdict (missed); a survivor is caught under the widened oracle iff one of the 167 lib tests features sqlite,remote,bench-support add, or one test of an integration target entering its file, fails. A file's widened figure is its `--lib` caught plus its survivors caught here, over its viable mutants (score = caught / (caught + missed + timeout), unviable excluded, as `tools/mutants-score`). A run whose test result lines are missing or whose invocation fails is voided and run again; a Missed verdict is kept only where every target ran (one result line for the lib and one per target).

- the Studio: lane p5-mutants-floors, 13 survivors, 2026-11-02T12:48:48Z to 13:27:23Z (the voided opening catches in void-1; stopped at 14:12:27Z by the host load)
- the box: lane p5-mutants-floors-remeasure, 204 survivors, 2026-11-02T14:24:18Z to 15:29:49Z (1.09 h), two workers, three from 14:27:36Z, the one-minute load 15.12 to 40.39 at the claims; voided: none

## Per file

| File | Viable | `--lib` caught | `--lib` | Newly caught | Widened caught | Widened | Floor | Widened oracle's targets |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `src/monitor/tasks/reorg.rs` | 24 | 2 | 8.33% | 21 | 23 | 95.83% | 80 | monitor_tests, reorg_tests |
| `src/monitor/tasks/review_proven_txs.rs` | 25 | 3 | 12.00% | 0 | 3 | 12.00% | 80 | monitor_tests |
| `src/services/providers/bhs.rs` | 12 | 8 | 66.67% | 0 | 8 | 66.67% | 80 | fixtures |
| `src/services/providers/bhs.rs` after #602 | 12 | 8 | 66.67% | 4 | 12 | 100.00% | 80 | fixtures and the rest of the door oracle `.cargo/mutants-doors.toml`, the section after #602 |
| `src/storage/sqlx/create_action/beef_storage.rs` | 77 | 37 | 48.05% | 0 | 37 | 48.05% | 80 | arc_broadcast_body_tests, caller_complete_funding_tests, fixtures, immediate_broadcast_hint_tests, known_ancestor_tests, monitor_tests |
| `src/storage/sqlx/create_action/beef_walk.rs` | 22 | 16 | 72.73% | 0 | 16 | 72.73% | 80 | arc_broadcast_body_tests, caller_complete_funding_tests, fixtures, immediate_broadcast_hint_tests, known_ancestor_tests, monitor_tests |
| `src/storage/sqlx/create_action/beef_walk.rs` after #601 | 22 | 16 | 72.73% | 5 | 21 | 95.45% | 80 | arc_broadcast_body_tests, caller_complete_funding_tests, fixtures, immediate_broadcast_hint_tests, known_ancestor_tests, monitor_tests (the whole catalogue `.cargo/mutants-evidence.toml`, the section after #601) |
| `src/storage/sqlx/create_action/mod.rs` | 37 | 23 | 62.16% | 7 | 30 | 81.08% | 80 | caller_complete_funding_tests, fixtures, immediate_broadcast_hint_tests, known_ancestor_tests, test_vectors |
| `src/storage/sqlx/internalize_action.rs` | 91 | 67 | 73.63% | 5 | 72 | 79.12% | 80 | fixtures, immediate_broadcast_hint_tests, internalize_custom_instructions_tests |
| `src/storage/sqlx/process_action.rs` | 222 | 137 | 61.71% | 16 | 153 | 68.92% | 80 | caller_complete_funding_tests, concurrent_tests, fixtures, immediate_broadcast_hint_tests, known_ancestor_tests, monitor_tests, poisoned_chain_tests, retire_rejected_tests |

Survivors run: 217; caught under the widened oracle: 49; still missed: 168; timeouts: 0; voided on the box: 0. Reaching 80 under the widened oracle: `src/monitor/tasks/reorg.rs`, `src/storage/sqlx/create_action/mod.rs`; their raised floors are measured there, not red by design, and stay red under the `--lib` oracle the workflow scores until their survivors are killed there.

## Still missed under the widened oracle

### `src/monitor/tasks/reorg.rs` (1)

- `src/monitor/tasks/reorg.rs:196:9: replace <impl MonitorTask for ReorgTask<S, V>>::run -> Result<TaskResult> with Ok(Default::default())`

### `src/monitor/tasks/review_proven_txs.rs` (22)

- `src/monitor/tasks/review_proven_txs.rs:126:45: replace match guard root.eq_ignore_ascii_case(&anchor.merkle_root) with true in ReviewProvenTxsTask<S, V>::review`
- `src/monitor/tasks/review_proven_txs.rs:126:45: replace match guard root.eq_ignore_ascii_case(&anchor.merkle_root) with false in ReviewProvenTxsTask<S, V>::review`
- `src/monitor/tasks/review_proven_txs.rs:135:46: replace += with -= in ReviewProvenTxsTask<S, V>::review`
- `src/monitor/tasks/review_proven_txs.rs:135:46: replace += with *= in ReviewProvenTxsTask<S, V>::review`
- `src/monitor/tasks/review_proven_txs.rs:186:9: replace <impl MonitorTask for ReviewProvenTxsTask<S, V>>::name -> &'static str with ""`
- `src/monitor/tasks/review_proven_txs.rs:186:9: replace <impl MonitorTask for ReviewProvenTxsTask<S, V>>::name -> &'static str with "xyzzy"`
- `src/monitor/tasks/review_proven_txs.rs:190:9: replace <impl MonitorTask for ReviewProvenTxsTask<S, V>>::default_interval -> Duration with Default::default()`
- `src/monitor/tasks/review_proven_txs.rs:190:32: replace * with + in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::default_interval`
- `src/monitor/tasks/review_proven_txs.rs:190:32: replace * with / in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::default_interval`
- `src/monitor/tasks/review_proven_txs.rs:194:9: replace <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run -> Result<TaskResult> with Ok(Default::default())`
- `src/monitor/tasks/review_proven_txs.rs:202:23: replace == with != in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:214:21: replace || with && in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:213:21: replace || with && in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:212:33: replace > with == in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:212:33: replace > with < in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:212:33: replace > with >= in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:213:49: replace > with == in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:213:49: replace > with < in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:213:49: replace > with >= in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:214:24: delete ! in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:232:55: replace + with - in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`
- `src/monitor/tasks/review_proven_txs.rs:232:55: replace + with * in <impl MonitorTask for ReviewProvenTxsTask<S, V>>::run`

### `src/services/providers/bhs.rs` (4)

- `src/services/providers/bhs.rs:123:12: delete ! in BlockHeaderService::current_height` (killed by #602, the section below)
- `src/services/providers/bhs.rs:165:9: replace BlockHeaderService::is_valid_root_for_height -> Result<bool> with Ok(true)` (killed by #602, the section below)
- `src/services/providers/bhs.rs:165:9: replace BlockHeaderService::is_valid_root_for_height -> Result<bool> with Ok(false)` (killed by #602, the section below)
- `src/services/providers/bhs.rs:166:31: replace == with != in BlockHeaderService::is_valid_root_for_height` (killed by #602, the section below)

### `src/storage/sqlx/create_action/beef_storage.rs` (40)

- `src/storage/sqlx/create_action/beef_storage.rs:28:21: replace < with <= in parse_input_txids`
- `src/storage/sqlx/create_action/beef_storage.rs:36:24: replace > with == in parse_input_txids`
- `src/storage/sqlx/create_action/beef_storage.rs:36:24: replace > with >= in parse_input_txids`
- `src/storage/sqlx/create_action/beef_storage.rs:81:24: replace > with < in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:81:20: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:89:21: replace == with != in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:90:24: replace > with == in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:90:24: replace > with < in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:90:24: replace > with >= in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:90:20: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:90:20: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:97:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:97:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:98:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:98:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:99:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:99:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:101:17: replace += with -= in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:101:17: replace += with *= in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:104:24: replace > with == in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:104:24: replace > with < in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:104:24: replace > with >= in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:104:20: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:104:20: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:111:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:111:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:112:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:112:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:113:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:113:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:114:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:114:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:115:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:115:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:116:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:116:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:117:26: replace + with - in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:117:26: replace + with * in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:119:17: replace += with -= in read_var_int_for_beef`
- `src/storage/sqlx/create_action/beef_storage.rs:119:17: replace += with *= in read_var_int_for_beef`

### `src/storage/sqlx/create_action/beef_walk.rs` (6)

- `src/storage/sqlx/create_action/beef_walk.rs:146:65: replace > with >= in beef_bfs_walk` (killed by #601, the section below)
- `src/storage/sqlx/create_action/beef_walk.rs:152:40: replace -= with += in beef_bfs_walk` (killed by #601, the section below)
- `src/storage/sqlx/create_action/beef_walk.rs:152:40: replace -= with /= in beef_bfs_walk` (killed by #601, the section below)
- `src/storage/sqlx/create_action/beef_walk.rs:180:32: delete ! in beef_bfs_walk` (killed by #601, the section below)
- `src/storage/sqlx/create_action/beef_walk.rs:307:29: replace && with || in beef_bfs_walk` (declared equivalent by #601, the section below)
- `src/storage/sqlx/create_action/beef_walk.rs:307:69: replace == with != in beef_bfs_walk` (killed by #601, the section below)

### `src/storage/sqlx/create_action/mod.rs` (7) (five killed by #604, 311:9 and 713:27 equivalent; the section after #604)

- `src/storage/sqlx/create_action/mod.rs:203:47: replace || with && in create_action_at_door_internal`
- `src/storage/sqlx/create_action/mod.rs:311:9: replace <impl std::fmt::Display for Again>::fmt -> std::fmt::Result with Ok(Default::default())`
- `src/storage/sqlx/create_action/mod.rs:339:9: replace Door<'_>::is_delayed -> bool with true`
- `src/storage/sqlx/create_action/mod.rs:339:9: replace Door<'_>::is_delayed -> bool with false`
- `src/storage/sqlx/create_action/mod.rs:399:13: delete ! in Door<'_>::step`
- `src/storage/sqlx/create_action/mod.rs:713:27: replace match guard !created.inputs.is_empty() with true in Door<'_>::step`
- `src/storage/sqlx/create_action/mod.rs:738:21: delete field is_new_tx from struct StorageProcessActionArgs expression in Door<'_>::step`

### `src/storage/sqlx/internalize_action.rs` (19)

- `src/storage/sqlx/internalize_action.rs:217:21: replace && with || in internalize_action_internal`
- `src/storage/sqlx/internalize_action.rs:229:68: replace && with || in internalize_action_internal`
- `src/storage/sqlx/internalize_action.rs:437:21: replace && with || in internalize_action_internal`
- `src/storage/sqlx/internalize_action.rs:436:46: replace == with != in internalize_action_internal`
- `src/storage/sqlx/internalize_action.rs:611:35: replace && with || in internalize_action_internal`
- `src/storage/sqlx/internalize_action.rs:703:9: delete match arm "unprocessed" in parse_transaction_status`
- `src/storage/sqlx/internalize_action.rs:706:9: delete match arm "unsigned" in parse_transaction_status`
- `src/storage/sqlx/internalize_action.rs:708:9: delete match arm "nonfinal" in parse_transaction_status`
- `src/storage/sqlx/internalize_action.rs:709:9: delete match arm "failed" in parse_transaction_status`
- `src/storage/sqlx/internalize_action.rs:710:9: delete match arm "unfail" in parse_transaction_status`
- `src/storage/sqlx/internalize_action.rs:1410:46: delete ! in broadcast_new_internalized_req`
- `src/storage/sqlx/internalize_action.rs:1415:21: replace || with && in broadcast_new_internalized_req`
- `src/storage/sqlx/internalize_action.rs:1417:25: replace && with || in broadcast_new_internalized_req`
- `src/storage/sqlx/internalize_action.rs:1416:25: replace && with || in broadcast_new_internalized_req`
- `src/storage/sqlx/internalize_action.rs:1415:25: delete ! in broadcast_new_internalized_req`
- `src/storage/sqlx/internalize_action.rs:1416:28: delete ! in broadcast_new_internalized_req`
- `src/storage/sqlx/internalize_action.rs:1417:54: replace || with && in broadcast_new_internalized_req`
- `src/storage/sqlx/internalize_action.rs:1420:39: replace || with && in broadcast_new_internalized_req`
- `src/storage/sqlx/internalize_action.rs:1520:5: replace mark_internalized_tx_failed -> Result<()> with Ok(())`

### `src/storage/sqlx/process_action.rs` (69)

- `src/storage/sqlx/process_action.rs:159:20: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:167:21: replace == with != in read_var_int`
- `src/storage/sqlx/process_action.rs:168:24: replace > with == in read_var_int`
- `src/storage/sqlx/process_action.rs:168:24: replace > with < in read_var_int`
- `src/storage/sqlx/process_action.rs:168:24: replace > with >= in read_var_int`
- `src/storage/sqlx/process_action.rs:168:20: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:168:20: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:175:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:175:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:176:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:176:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:177:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:177:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:179:17: replace += with -= in read_var_int`
- `src/storage/sqlx/process_action.rs:179:17: replace += with *= in read_var_int`
- `src/storage/sqlx/process_action.rs:182:24: replace > with == in read_var_int`
- `src/storage/sqlx/process_action.rs:182:24: replace > with < in read_var_int`
- `src/storage/sqlx/process_action.rs:182:24: replace > with >= in read_var_int`
- `src/storage/sqlx/process_action.rs:182:20: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:182:20: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:189:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:189:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:190:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:190:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:191:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:191:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:192:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:192:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:193:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:193:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:194:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:194:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:195:26: replace + with - in read_var_int`
- `src/storage/sqlx/process_action.rs:195:26: replace + with * in read_var_int`
- `src/storage/sqlx/process_action.rs:197:17: replace += with -= in read_var_int`
- `src/storage/sqlx/process_action.rs:197:17: replace += with *= in read_var_int`
- `src/storage/sqlx/process_action.rs:205:21: replace < with == in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:205:21: replace < with <= in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:214:24: replace > with == in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:214:24: replace > with >= in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:224:32: replace > with == in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:224:32: replace > with >= in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:231:23: replace > with == in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:231:23: replace > with >= in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:231:19: replace + with - in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:248:23: replace > with == in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:248:23: replace > with >= in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:258:32: replace > with == in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:258:32: replace > with >= in parse_tx_script_offsets`
- `src/storage/sqlx/process_action.rs:313:5: replace find_transaction_by_txid -> Result<Option<TransactionRecord>> with Ok(None)`
- `src/storage/sqlx/process_action.rs:392:38: replace > with == in update_output_with_script_offset`
- `src/storage/sqlx/process_action.rs:392:38: replace > with >= in update_output_with_script_offset`
- `src/storage/sqlx/process_action.rs:671:14: replace match guard transaction_status == "failed" with false in find_stored_post`
- `src/storage/sqlx/process_action.rs:673:14: replace match guard transaction_status == "completed" with false in find_stored_post`
- `src/storage/sqlx/process_action.rs:675:16: replace match guard raw_tx.is_some() && input_beef.is_some() with true in find_stored_post`
- `src/storage/sqlx/process_action.rs:675:33: replace && with || in find_stored_post`
- `src/storage/sqlx/process_action.rs:834:24: replace && with || in post_stored_actions`
- `src/storage/sqlx/process_action.rs:942:31: replace += with *= in post_note_counts`
- `src/storage/sqlx/process_action.rs:946:69: replace == with != in post_note_counts`
- `src/storage/sqlx/process_action.rs:950:21: replace += with *= in post_note_counts`
- `src/storage/sqlx/process_action.rs:952:26: replace += with -= in post_note_counts`
- `src/storage/sqlx/process_action.rs:952:26: replace += with *= in post_note_counts`
- `src/storage/sqlx/process_action.rs:954:27: replace += with -= in post_note_counts`
- `src/storage/sqlx/process_action.rs:954:27: replace += with *= in post_note_counts`
- `src/storage/sqlx/process_action.rs:956:26: replace += with -= in post_note_counts`
- `src/storage/sqlx/process_action.rs:956:26: replace += with *= in post_note_counts`
- `src/storage/sqlx/process_action.rs:1065:13: replace || with && in process_action_writes`
- `src/storage/sqlx/process_action.rs:1204:17: replace || with && in process_action_writes`
- `src/storage/sqlx/process_action.rs:1203:17: replace || with && in process_action_writes`

## After #601: `beef_walk.rs` at 21 of 22 under its reach targets

The lane p5-601-beef-walk-survivors, on beta `047ef756`, the tree at `e7e93351` (the three tests) with the catalogue of `b8b75158`; the json under `issue_601` (the complete record's json carries the same key), the outputs under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-601-beef-walk-survivors`. The oracle is the one whose targets enter the file: `.cargo/mutants-evidence.toml`, the lib and the six targets of the row above, features `sqlite,remote,bench-support`, the whole of each, not the filtered selection of the method above. `beef_walk.rs` is unchanged; each kill is a lib test from the rule, red under the mutant planted by hand and green at the tip, then caught again by cargo-mutants.

**146:65 and 152:40 (`>` to `>=`; `-=` to `+=` and to `/=`), killed.** The rule: one walk asks services for at most `MAX_PROOF_FETCHES_PER_WALK` (8) merkle paths, the cap on the case where nothing on the chain is mined yet. The witness, `storage::sqlx::create_action::beef_walk::beef_walk_ancestry::test_walk_asks_for_at_most_its_proof_budget`: ten old unproven hops above a proven root, a tracker on the storage, every ask answered with a service error. At the tip: 8 asks and every hop carried. Under `>=` the ninth hop subtracts from a zero budget (`attempt to subtract with overflow` at 152:21); under `+=` and `/=` the budget never runs out: 10 asks.

**180:32 (`delete !`), killed.** The rule: a stored input_beef that does not hold its subject is pruned to the closure of the subject's inputs, so a stored transaction the subject never spends does not reach the BEEF. The witness, `storage::sqlx::create_action::beef_walk::beef_walk_stored::test_walk_keeps_the_closure_of_the_inputs_when_the_stored_beef_lacks_its_subject`: a stored BEEF of the subject's proven parent and a proven stray. At the tip the stray is pruned; under the mutant the roots stay the absent subject, nothing is pruned, and the stray is carried.

**307:69 (`==` to `!=`), killed.** The rule: every input of an unproven transaction is walked, whatever is already queued. The witness, `storage::sqlx::create_action::beef_walk::beef_walk_ancestry::test_walk_queues_every_input_of_an_unproven_transaction`: an unproven subject spending two proven parents. At the tip both parents and their BUMPs are carried; under the mutant the right parent is not queued (the left one, queued, differs from it) and is missing.

**307:29 (`&&` to `||`), equivalent.** The rule (the same line): an input is queued unless already walked or already queued. Under `||` an input already walked, or already queued, is queued again. Every entry the walk pops meets the `processed_txids` check at the loop's head before any read, merge or write, and the earlier entry of the same txid sits ahead of the copy (pushes append, pops take the front), so the copy is popped after the txid is in `processed_txids` and dropped. A copy carries `depth + 1`, never 0, so the direct-input refusal reads the earlier entry's depth. The four callers (`build_input_beef`, `rebuild_beef_for_broadcast`, `change_coin_gap` in `beef.rs` and the BEEF of `list_outputs`) build `pending` and `processed` for the one call and read neither after it, an `Err` returned or mapped without them. The BEEF, the refusals and the stored rows agree; only the queue's length differs, bounded by the inputs of the transactions walked. cargo-mutants counts an equivalent as missed, and the figure below counts it so.

The file under that oracle (`cargo mutants --config .cargo/mutants-evidence.toml --timeout 900`, copy mode, serial, six round-robin shards run one after another, shard 0 with the unmutated baseline: 64 s build, 21 s test, the others `--baseline skip`), 2026-11-04T02:05:51Z to 02:26:58Z: 23 mutants, 21 caught, 1 missed, 1 unviable, 0 timeout. Score 95.45% (21/22) against the floor of 80; `tools/mutants-score --baseline-skipped` over the six outcomes with the catalogue's `--list --json`: PASS.

| Mutant | Verdict |
|---|---|
| `src/storage/sqlx/create_action/beef_walk.rs:57:5: replace stored_bump_refuted -> bool with true` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:57:5: replace stored_bump_refuted -> bool with false` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:80:5: replace beef_bfs_walk -> Result<()> with Ok(())` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:85:9: delete match arm (None, Some(s)) in beef_bfs_walk` | Unviable |
| `src/storage/sqlx/create_action/beef_walk.rs:146:43: replace && with || in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:146:65: replace > with == in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:146:65: replace > with < in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:146:65: replace > with >= in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:149:36: replace >= with < in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:152:40: replace -= with += in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:152:40: replace -= with /= in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:167:12: delete ! in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:180:32: delete ! in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:256:39: replace == with != in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:259:29: replace && with || in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:258:46: replace != with == in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:307:29: replace && with || in beef_bfs_walk` | MissedMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:306:28: delete ! in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:307:32: delete ! in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:307:69: replace == with != in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:309:67: replace + with - in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:309:67: replace + with * in beef_bfs_walk` | CaughtMutant |
| `src/storage/sqlx/create_action/beef_walk.rs:317:22: replace == with != in beef_bfs_walk` | CaughtMutant |

## After #602: `bhs.rs` at 12 of 12 under the door oracle

The lane p5-602-bhs-survivors, on beta `88a93492`, the tree at `d17e8ca9` (the two tests) with the catalogue of `151d2d77`; the json under `issue_602` (the complete record's, the widened record's and the door oracle record's json carry the same key), the outputs under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-602-bhs-survivors` (`run`, `run.log`, `catalog-lib.json`, `catalog-doors.json`). The oracle is the one whose targets enter the file: the file joins `.cargo/mutants-doors.toml` (the header service's `is_valid_root_for_height`, the chain's evidence the tracker's verdict reads, docs/ARCHITECTURE.md section 4 step 2; `fixtures` the one integration target that enters it) and leaves `.cargo/mutants.toml`'s lib set. `bhs.rs`'s rules are unchanged; each kill is a lib test in the file's own test module from the rule, red under the mutant planted by hand and green at the tip, then caught again by cargo-mutants.

**123:12 (`delete !` in `current_height`), killed.** The rule (the line's guard): a non-success status is the service's error, never its body read as a height. The witness, `services::providers::bhs::tests::current_height_is_a_successful_answer_and_a_failed_status_is_an_error`: a 200 with the body `900002` answers 900002, and a 503 with the same body is `BHS current_height failed: HTTP 503`. Under the mutant the 200 is the error, red at the success's unwrap.

**165:9 (the function replaced by `Ok(true)` and by `Ok(false)`) and 166:31 (`==` to `!=`), killed.** The rule: a root is valid for a height when it is the merkle root of the service's header at that height, and invalid when it is any other. The witness, `services::providers::bhs::tests::is_valid_root_for_height_is_the_root_of_the_header_at_the_height`: against a header at 900002 whose root is `c` repeated, that root answers true and the root `e` repeated answers false. `Ok(false)` and `!=` fail the matching root, `Ok(true)` the mismatching one.

The file under the door oracle (given with `--no-config`, the doors config's flags by hand, in place, serial, timeouts 900 s): `17 mutants tested in 11m: 12 caught, 5 unviable`, 100.00% (12/12), no timeout to judge against the baseline (0 s build + 18 s test). Before (the complete record's `--lib` and the widened re-measure): 66.67% (8/12). The floor stays 80. The catalogues listed on the tree: the lib set 2218 (2235 before), the doors 85 (68 before, `bhs.rs` 17).

## After #604: `create_action/mod.rs` under the door oracle

The lane p5-604-create-door-survivors, on beta `b1879b8d` with #602's lane commits (base `b044407f`), the tree at `2fb239d5` (the five tests, `create_action/tests/door_steps.rs`) with the catalogue of `7313d361`; the json under `issue_604` (the complete record's, the widened record's and the door oracle record's json carry the same key), the outputs under `/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-604-create-door-survivors` (`survivors`, `file/s0` to `file/s3`, `verdicts.json`). The oracle is the door oracle (`.cargo/mutants-doors.toml`, the file in its examine_globs since #604): the lib and the six targets, features `sqlite,remote,bench-support`; the five targets that enter the file (caller_complete_funding_tests, fixtures, immediate_broadcast_hint_tests, known_ancestor_tests, test_vectors) are all among them.

**339:9 (`is_delayed` replaced by `false` and by `true`), killed.** The rule: the delayed route may take a change coin not yet marked spendable, the immediate route only a coin marked spendable (the allocation's `require_spendable` is `!is_delayed`). The witnesses, `the_delayed_route_may_spend_a_change_coin_not_marked_spendable` (red under `false`: the delayed action is refused `InsufficientFunds`) and `the_immediate_route_spends_only_a_change_coin_marked_spendable` (red under `true`: the immediate action spends the unmarked coin).

**399:13 (`delete !` in `Door::step`), killed.** The rule: the write-locked step counts the change coins its route may spend, as the pre-check does, so an unmarked coin never stands in for a change output the basket wants. The witness, `the_step_counts_only_the_change_coins_its_route_may_spend`: with the basket wanting two coins, an immediate action makes as many change outputs beside an unmarked coin as without it; under the mutant it makes fewer.

**203:47 (`||` to `&&` in `create_action_at_door_internal`), killed.** The rule: with no caller tracker the step's assembly reads every stored proof against the storage's own tracker's answers (`reads_proofs`), so a checked stored bump whose root that tracker refutes never enters the BEEF (ruling 7c). The witness, `without_a_caller_tracker_the_step_reads_stored_proofs_against_the_storages_own`: the coin's transaction rides without its refuted bump, above its proven parent; under the mutant the step merges the refuted bump.

**738:21 (the field `is_new_tx` deleted), killed.** The rule: `SignAndProcess`'s contract, the door sets `is_new_tx` itself (with the reference, the txid and the raw bytes) whatever the host's flags say. The witness, `the_door_processes_its_signed_action_as_a_new_transaction`: a host passing `is_new_tx: false` still has its signed bytes stored by processAction's new-transaction writes; under the mutant they are not.

**311:9 (`Again`'s Display replaced by an empty answer), equivalent.** The text is read only by the `reason` field of the `create_action_precheck_again` log; every refusal built from an `Again` is the match's own error (`ServiceError` naming the height, `StoreBusy { MovedUnderUs }`), never the Display. No caller, response or stored row reads it.

**713:27 (the guard `!created.inputs.is_empty()` replaced by `true`), equivalent.** The step reaches the guard only after `plan_change` returned a plan, and every plan carries an input: the fee is at least 2 sats (a transaction is at least 10 bytes at 101 sat/kB, rounded up), so an action with no input is short and `generate_change` refuses `InsufficientFunds`; a caller-complete plan has a caller input. The two forms agree on every reachable call.

The file under the door oracle (given with `--no-config`, the doors config's flags by hand, in place, serial, four shards, timeouts 900 s): `39 mutants tested` (10, 10, 10, 9) in 33 minutes, 35 caught, 2 missed (the two equivalents, counted as missed as cargo-mutants counts them), 0 timeout, 2 unviable: 94.59% (35/37), no timeout to judge against the baseline (30 s build + 19 s test). Before: 62.16% (23/37) under `--lib`, 81.08% (30/37) under the widened re-measure. The floor stays 80. The catalogues listed on the tree: the lib set 2179 (2218 before), the doors 124 (85 before).
