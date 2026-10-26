use crate::error::Result;
use crate::storage::entities::{TableOutputBasket, TableOutputTag, TableTxLabel};
use crate::storage::traits::StorageProvidedBy;
use crate::wallet::RandomSource;
use chrono::Utc;
use sqlx::sqlite::SqliteConnection;
use sqlx::Row;

/// Extended output with additional context for transaction creation.
#[derive(Debug, Clone)]
pub(super) struct ExtendedOutput {
    pub(super) vout: u32,
    pub(super) satoshis: u64,
    pub(super) locking_script: Vec<u8>,
    pub(super) output_description: String,
    pub(super) basket: Option<String>,
    pub(super) tags: Vec<String>,
    pub(super) custom_instructions: Option<String>,
    pub(super) provided_by: StorageProvidedBy,
    pub(super) purpose: Option<String>,
    pub(super) derivation_suffix: Option<String>,
}

/// Validates and extends output specifications.
pub(super) fn validate_and_extend_outputs(
    args: &bsv_rs::wallet::CreateActionArgs,
) -> Result<Vec<ExtendedOutput>> {
    let mut extended = Vec::new();

    if let Some(ref outputs) = args.outputs {
        for (i, output) in outputs.iter().enumerate() {
            extended.push(ExtendedOutput {
                vout: i as u32,
                satoshis: output.satoshis,
                locking_script: output.locking_script.clone(),
                output_description: output.output_description.clone(),
                basket: output.basket.clone(),
                tags: output.tags.clone().unwrap_or_default(),
                custom_instructions: output.custom_instructions.clone(),
                provided_by: StorageProvidedBy::You,
                purpose: None,
                derivation_suffix: None,
            });
        }
    }

    Ok(extended)
}

/// Creates a new transaction record.
pub(super) async fn create_transaction_record(
    conn: &mut SqliteConnection,
    user_id: i64,
    args: &bsv_rs::wallet::CreateActionArgs,
    rng: &RandomSource,
) -> Result<i64> {
    let now = Utc::now();
    let reference = rng.base64(12);
    let version = args.version.unwrap_or(1) as i32;
    let lock_time = args.lock_time.unwrap_or(0) as i64;

    let result = sqlx::query(
        r#"
        INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, created_at, updated_at)
        VALUES (?, 'unsigned', ?, 1, 0, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(user_id)
    .bind(&reference)
    .bind(version)
    .bind(lock_time)
    .bind(&args.description)
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(result.last_insert_rowid())
}

/// Gets the reference for a transaction.
pub(super) async fn get_transaction_reference(
    conn: &mut SqliteConnection,
    transaction_id: i64,
) -> Result<String> {
    let row = sqlx::query("SELECT reference FROM transactions WHERE transaction_id = ?")
        .bind(transaction_id)
        .fetch_one(&mut *conn)
        .await?;

    Ok(row.get("reference"))
}

/// Updates transaction satoshis.
pub(super) async fn update_transaction_satoshis(
    conn: &mut SqliteConnection,
    transaction_id: i64,
    satoshis: i64,
) -> Result<()> {
    let now = Utc::now();

    sqlx::query("UPDATE transactions SET satoshis = ?, updated_at = ? WHERE transaction_id = ?")
        .bind(satoshis)
        .bind(now)
        .bind(transaction_id)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// Finds or creates a transaction label.
pub(super) async fn find_or_insert_tx_label(
    conn: &mut SqliteConnection,
    user_id: i64,
    label: &str,
) -> Result<TableTxLabel> {
    let row = sqlx::query(
        "SELECT tx_label_id, user_id, label, created_at, updated_at FROM tx_labels WHERE user_id = ? AND label = ?",
    )
    .bind(user_id)
    .bind(label)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = row {
        return Ok(TableTxLabel {
            label_id: row.get("tx_label_id"),
            user_id: row.get("user_id"),
            label: row.get("label"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        });
    }

    let now = Utc::now();
    let result = sqlx::query(
        "INSERT INTO tx_labels (user_id, label, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(label)
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(TableTxLabel {
        label_id: result.last_insert_rowid(),
        user_id,
        label: label.to_string(),
        created_at: now,
        updated_at: now,
    })
}

/// Finds or creates a transaction label map entry.
pub(super) async fn find_or_insert_tx_label_map(
    conn: &mut SqliteConnection,
    transaction_id: i64,
    label_id: i64,
) -> Result<i64> {
    let row = sqlx::query(
        "SELECT tx_label_map_id FROM tx_labels_map WHERE transaction_id = ? AND tx_label_id = ?",
    )
    .bind(transaction_id)
    .bind(label_id)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = row {
        return Ok(row.get("tx_label_map_id"));
    }

    let now = Utc::now();
    let result = sqlx::query(
        "INSERT INTO tx_labels_map (transaction_id, tx_label_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(transaction_id)
    .bind(label_id)
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(result.last_insert_rowid())
}

/// Finds or creates an output basket. The read runs on `conn`, the caller's
/// write transaction, so a basket inserted earlier in that transaction (an
/// earlier output naming the same new basket) is found, not inserted twice.
pub(in crate::storage::sqlx) async fn find_or_insert_output_basket(
    conn: &mut SqliteConnection,
    user_id: i64,
    name: &str,
) -> Result<TableOutputBasket> {
    let row = sqlx::query(
        r#"
        SELECT basket_id, user_id, name, number_of_desired_utxos, minimum_desired_utxo_value,
               created_at, updated_at
        FROM output_baskets
        WHERE user_id = ? AND name = ? AND is_deleted = 0
        ORDER BY updated_at ASC
        LIMIT 1
        "#,
    )
    .bind(user_id)
    .bind(name)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = row {
        return Ok(TableOutputBasket {
            basket_id: row.get("basket_id"),
            user_id: row.get("user_id"),
            name: row.get("name"),
            number_of_desired_utxos: row.get("number_of_desired_utxos"),
            minimum_desired_utxo_value: row.get("minimum_desired_utxo_value"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        });
    }

    let now = Utc::now();
    let result = sqlx::query(
        r#"
        INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at)
        VALUES (?, ?, 6, 10000, ?, ?)
        "#,
    )
    .bind(user_id)
    .bind(name)
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(TableOutputBasket {
        basket_id: result.last_insert_rowid(),
        user_id,
        name: name.to_string(),
        number_of_desired_utxos: 6,
        minimum_desired_utxo_value: 10000,
        created_at: now,
        updated_at: now,
    })
}

/// Finds or creates an output tag.
pub(super) async fn find_or_insert_output_tag(
    conn: &mut SqliteConnection,
    user_id: i64,
    tag: &str,
) -> Result<TableOutputTag> {
    let row = sqlx::query(
        "SELECT output_tag_id, user_id, tag, created_at, updated_at FROM output_tags WHERE user_id = ? AND tag = ?",
    )
    .bind(user_id)
    .bind(tag)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = row {
        return Ok(TableOutputTag {
            tag_id: row.get("output_tag_id"),
            user_id: row.get("user_id"),
            tag: row.get("tag"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        });
    }

    let now = Utc::now();
    let result = sqlx::query(
        "INSERT INTO output_tags (user_id, tag, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(tag)
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(TableOutputTag {
        tag_id: result.last_insert_rowid(),
        user_id,
        tag: tag.to_string(),
        created_at: now,
        updated_at: now,
    })
}

/// Finds or creates an output tag map entry.
pub(super) async fn find_or_insert_output_tag_map(
    conn: &mut SqliteConnection,
    output_id: i64,
    tag_id: i64,
) -> Result<i64> {
    let row = sqlx::query(
        "SELECT output_tag_map_id FROM output_tags_map WHERE output_id = ? AND output_tag_id = ?",
    )
    .bind(output_id)
    .bind(tag_id)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(row) = row {
        return Ok(row.get("output_tag_map_id"));
    }

    let now = Utc::now();
    let result = sqlx::query(
        "INSERT INTO output_tags_map (output_id, output_tag_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(output_id)
    .bind(tag_id)
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(result.last_insert_rowid())
}

/// Inserts a new output record.
#[allow(clippy::too_many_arguments)]
pub(super) async fn insert_output(
    conn: &mut SqliteConnection,
    user_id: i64,
    transaction_id: i64,
    basket_id: Option<i64>,
    vout: i32,
    satoshis: i64,
    locking_script: &[u8],
    output_description: &str,
    provided_by: &StorageProvidedBy,
    purpose: &str,
    custom_instructions: Option<&str>,
    derivation_prefix: Option<&str>,
    derivation_suffix: Option<&str>,
    change: bool,
    spendable: bool,
) -> Result<i64> {
    let now = Utc::now();
    let provided_by_str = match provided_by {
        StorageProvidedBy::You => "you",
        StorageProvidedBy::Storage => "storage",
        StorageProvidedBy::YouAndStorage => "you-and-storage",
    };

    let output_type = if change { "P2PKH" } else { "custom" };
    let script_to_store = if locking_script.is_empty() {
        None
    } else {
        Some(locking_script)
    };

    let result = sqlx::query(
        r#"
        INSERT INTO outputs (
            user_id, transaction_id, basket_id, vout, satoshis, locking_script,
            output_description, provided_by, purpose, type, custom_instructions,
            derivation_prefix, derivation_suffix, change, spendable, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(user_id)
    .bind(transaction_id)
    .bind(basket_id)
    .bind(vout)
    .bind(satoshis)
    .bind(script_to_store)
    .bind(output_description)
    .bind(provided_by_str)
    .bind(purpose)
    .bind(output_type)
    .bind(custom_instructions)
    .bind(derivation_prefix)
    .bind(derivation_suffix)
    .bind(change)
    .bind(spendable)
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(result.last_insert_rowid())
}
#[cfg(test)]
#[path = "tests/outputs.rs"]
mod tests;
