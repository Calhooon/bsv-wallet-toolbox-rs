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
| `src/storage/sqlx/create_action/beef_storage.rs` | 77 | 37 | 48.05% | 0 | 37 | 48.05% | 80 | arc_broadcast_body_tests, caller_complete_funding_tests, fixtures, immediate_broadcast_hint_tests, known_ancestor_tests, monitor_tests |
| `src/storage/sqlx/create_action/beef_walk.rs` | 22 | 16 | 72.73% | 0 | 16 | 72.73% | 80 | arc_broadcast_body_tests, caller_complete_funding_tests, fixtures, immediate_broadcast_hint_tests, known_ancestor_tests, monitor_tests |
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

- `src/services/providers/bhs.rs:123:12: delete ! in BlockHeaderService::current_height`
- `src/services/providers/bhs.rs:165:9: replace BlockHeaderService::is_valid_root_for_height -> Result<bool> with Ok(true)`
- `src/services/providers/bhs.rs:165:9: replace BlockHeaderService::is_valid_root_for_height -> Result<bool> with Ok(false)`
- `src/services/providers/bhs.rs:166:31: replace == with != in BlockHeaderService::is_valid_root_for_height`

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

- `src/storage/sqlx/create_action/beef_walk.rs:146:65: replace > with >= in beef_bfs_walk`
- `src/storage/sqlx/create_action/beef_walk.rs:152:40: replace -= with += in beef_bfs_walk`
- `src/storage/sqlx/create_action/beef_walk.rs:152:40: replace -= with /= in beef_bfs_walk`
- `src/storage/sqlx/create_action/beef_walk.rs:180:32: delete ! in beef_bfs_walk`
- `src/storage/sqlx/create_action/beef_walk.rs:307:29: replace && with || in beef_bfs_walk`
- `src/storage/sqlx/create_action/beef_walk.rs:307:69: replace == with != in beef_bfs_walk`

### `src/storage/sqlx/create_action/mod.rs` (7)

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
