use anyhow::Result;
use serde_json::json;

use super::{model::Fixture, rows, seed, Replay};

async fn receipt(proven: bool) -> Result<Replay> {
    let mut fixture: Fixture = serde_json::from_str(include_str!(
        "../data/storage/create_received_unproven_without_rule.json"
    ))?;
    fixture.inputs.transactions[1].proven = proven;
    let mut replay = seed::setup(&fixture.inputs).await?;
    let accepted = replay.execute(&fixture.calls[0]).await?;
    assert!(accepted.get("ok").is_some(), "{accepted}");
    Ok(replay)
}

async fn spend(replay: &mut Replay, label: Option<&str>) -> Result<serde_json::Value> {
    let mut call = json!({
        "method": "create_action_offering",
        "as": "spend",
        "offer": ["receipt.0"],
        "args": {
            "description": "Spend received payment",
            "outputs": [{"lockingScript": "51", "satoshis": 1000,
                "outputDescription": "Recipient"}],
            "options": {"noSend": true, "randomizeOutputs": false,
                "signAndProcess": false}
        }
    });
    if let Some(label) = label {
        call["args"]["labels"] = json!([label]);
    }
    let before_calls = replay.services().call_history().len();
    let result = replay.execute(&serde_json::from_value(call)?).await?;
    assert_eq!(replay.services().call_history().len(), before_calls);
    Ok(result)
}

#[tokio::test]
async fn received_rule_is_exact_and_belongs_to_the_spending_action() -> Result<()> {
    for label in [
        None,
        Some("underReceivedRule "),
        Some("UNDERRECEIVEDRULE"),
        Some("spend-gate:received-pending"),
        Some("underReceivedRule"),
    ] {
        // The receipt already carries the label. Only this call can opt in.
        let mut replay = receipt(false).await?;
        let before = rows(&replay.storage).await?;
        let result = spend(&mut replay, label).await?;
        if label == Some("underReceivedRule") {
            assert!(result.get("ok").is_some(), "{result}");
            let labels: Vec<String> = sqlx::query_scalar(
                "SELECT l.label FROM tx_labels l \
                 JOIN tx_labels_map m ON m.tx_label_id = l.tx_label_id \
                 JOIN transactions t ON t.transaction_id = m.transaction_id \
                 WHERE t.is_outgoing = 1",
            )
            .fetch_all(replay.storage.pool())
            .await?;
            assert_eq!(labels, ["underreceivedrule"]);
        } else {
            assert!(
                result["error"]
                    .as_str()
                    .is_some_and(|error| error.starts_with("Insufficient funds")),
                "{result}"
            );
            assert_eq!(rows(&replay.storage).await?, before);
        }
    }
    Ok(())
}

#[tokio::test]
async fn received_mined_payment_needs_no_rule() -> Result<()> {
    let mut replay = receipt(true).await?;
    let result = spend(&mut replay, None).await?;
    assert!(result.get("ok").is_some(), "{result}");
    Ok(())
}
