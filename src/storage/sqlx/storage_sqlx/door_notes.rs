//! The internalize door's notes on a request (the fix round of the held
//! payment's answer): what each instance said to the door's post of a
//! held presentation that no instance took.
//!
//! A note is the door's (`"what": "doorPost"`) and names the user whose
//! presentation drew it (`"user"`) and the instance (`"instance"`). The
//! door keeps at most one note per instance on a request: the newest
//! replaces its own older one, so other users' presentations of the same
//! transaction never crowd the request's own notes out of the history's
//! bound. The request's sending clock (its `attempts` and `updated_at`,
//! which pace the send-waiting task's re-ask) moves only for a request this
//! presentation created: another user's presentation never slows or ends a
//! request's own sending. Its status: a request still to send (`unsent`,
//! `sending`, `unprocessed`) is left `unsent`; any other word stays.

use super::{Result, StorageSqlx, REQ_HISTORY_NOTES_KEPT};

/// The `what` of the door's note.
pub(crate) const DOOR_POST_NOTE: &str = "doorPost";

/// One instance's answer to the door's post, as its note records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstanceNote {
    /// The instance's name.
    pub(crate) instance: String,
    /// Its words.
    pub(crate) word: String,
}

/// `history` with the door's `notes` in place of the door's older note for
/// each of their instances, the newest kept within the history's bound.
fn replace_door_notes(history: &str, notes: Vec<serde_json::Value>) -> String {
    let mut object = match serde_json::from_str::<serde_json::Value>(history) {
        Ok(serde_json::Value::Object(map)) => map,
        _ if history.trim().is_empty() => serde_json::Map::new(),
        _ => {
            let mut map = serde_json::Map::new();
            map.insert("previous".to_string(), serde_json::Value::from(history));
            map
        }
    };
    let mut kept = match object.remove("notes") {
        Some(serde_json::Value::Array(kept)) => kept,
        _ => Vec::new(),
    };
    let instance_of = |n: &serde_json::Value| {
        (n.get("what").and_then(|w| w.as_str()) == Some(DOOR_POST_NOTE))
            .then(|| {
                n.get("instance")
                    .and_then(|i| i.as_str())
                    .map(str::to_string)
            })
            .flatten()
    };
    let replaced: Vec<String> = notes.iter().filter_map(instance_of).collect();
    kept.retain(|n| instance_of(n).is_none_or(|i| !replaced.contains(&i)));
    kept.extend(notes);
    let excess = kept.len().saturating_sub(REQ_HISTORY_NOTES_KEPT);
    kept.drain(..excess);
    object.insert("notes".to_string(), serde_json::Value::Array(kept));
    serde_json::Value::Object(object).to_string()
}

impl StorageSqlx {
    /// The door's post of `user`'s presentation of `txid` drew no take:
    /// each instance's note replaces the door's older one for it (see the
    /// module). `own_request` is whether this presentation created the
    /// request; only then does its sending clock move. A named competitor
    /// is queued for a proof ask.
    pub(in crate::storage::sqlx) async fn record_door_post_note(
        &self,
        txid: &str,
        user: i64,
        own_request: bool,
        outcome: &str,
        instances: &[InstanceNote],
        competitors: &[String],
    ) -> Result<()> {
        let row: Option<(i64, i64, String, String)> = sqlx::query_as(
            "SELECT proven_tx_req_id, attempts, history, status FROM proven_tx_reqs WHERE txid = ?",
        )
        .bind(txid)
        .fetch_optional(self.pool())
        .await?;
        let Some((proven_tx_req_id, attempts, history, status)) = row else {
            return Ok(());
        };
        let sends = matches!(status.as_str(), "unsent" | "sending" | "unprocessed");
        let clock = own_request && sends;
        let attempts = attempts.saturating_add(i64::from(clock));
        let now = chrono::Utc::now();
        let notes = instances
            .iter()
            .map(|i| {
                serde_json::json!({
                    "when": now.to_rfc3339(),
                    "what": DOOR_POST_NOTE,
                    "door": "internalize",
                    "user": user,
                    "instance": i.instance,
                    "outcome": outcome,
                    "word": i.word,
                })
            })
            .collect();
        let history = replace_door_notes(&history, notes);
        let status = if sends { "unsent" } else { status.as_str() };
        let sql = if clock {
            "UPDATE proven_tx_reqs SET status = ?, history = ?, attempts = ?, updated_at = ? WHERE proven_tx_req_id = ?"
        } else {
            "UPDATE proven_tx_reqs SET status = ?, history = ? WHERE proven_tx_req_id = ?"
        };
        let mut query = sqlx::query(sql).bind(status).bind(history);
        if clock {
            query = query.bind(attempts).bind(now);
        }
        query
            .bind(proven_tx_req_id)
            .execute(&mut *self.writer_conn().await?)
            .await?;
        self.queue_competitors(txid, competitors).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(user: i64, instance: &str, word: &str) -> serde_json::Value {
        serde_json::json!({"what": DOOR_POST_NOTE, "user": user, "instance": instance, "word": word})
    }

    #[test]
    fn the_door_keeps_one_note_per_instance() {
        let mut history = r#"{"notes":[{"what":"processActionPost"}]}"#.to_string();
        for user in 0..100 {
            history = replace_door_notes(
                &history,
                vec![note(user, "arcade", "no"), note(user, "bitails", "no")],
            );
        }
        let value: serde_json::Value = serde_json::from_str(&history).unwrap();
        let notes = value["notes"].as_array().unwrap();
        assert_eq!(notes.len(), 3, "{history}");
        assert_eq!(notes[0]["what"], "processActionPost");
        assert_eq!(notes[1]["user"], 99);
        assert_eq!(notes[2]["instance"], "bitails");
    }
}
