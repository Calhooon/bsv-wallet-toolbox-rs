use std::process::Command;

use anyhow::Result;
use serde_json::Value;

// Each database row occupies one line, including nested transaction views.
// This keeps large offers reviewable without splitting a fixture across files.
pub(super) fn format(value: &Value) -> String {
    fn render(value: &Value, depth: usize, rows: bool) -> String {
        let compact = serde_json::to_string(value).unwrap();
        if compact.len() <= 110 || (!value.is_object() && !value.is_array()) {
            return compact;
        }
        let indent = "  ".repeat(depth);
        let next = "  ".repeat(depth + 1);
        match value {
            Value::Array(items) => {
                let lines: Vec<_> = items
                    .iter()
                    .map(|item| {
                        let text = if rows {
                            serde_json::to_string(item).unwrap()
                        } else {
                            render(item, depth + 1, false)
                        };
                        format!("{next}{text}")
                    })
                    .collect();
                format!("[\n{}\n{indent}]", lines.join(",\n"))
            }
            Value::Object(fields) => {
                let lines: Vec<_> = fields
                    .iter()
                    .map(|(key, value)| {
                        format!(
                            "{next}{}: {}",
                            serde_json::to_string(key).unwrap(),
                            render(value, depth + 1, key == "rows" || rows)
                        )
                    })
                    .collect();
                format!("{{\n{}\n{indent}}}", lines.join(",\n"))
            }
            _ => compact,
        }
    }
    format!("{}\n", render(value, 0, false))
}

pub(super) fn diff(name: &str, expected: &Value, actual: &Value) -> Result<()> {
    let directory = tempfile::tempdir_in(std::env::temp_dir())?;
    let old = directory.path().join("expected.json");
    let new = directory.path().join("actual.json");
    std::fs::write(&old, format(expected))?;
    std::fs::write(&new, format(actual))?;
    let output = Command::new("diff")
        .args([
            "-u",
            "--label",
            &format!("{name}/expected"),
            "--label",
            &format!("{name}/actual"),
        ])
        .arg(old)
        .arg(new)
        .output()?;
    anyhow::ensure!(
        output.status.code() == Some(1),
        "diff could not compare: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

#[test]
fn row_format_keeps_large_offers_and_all_values() {
    let value = serde_json::json!({"rows": {"outputs": (0..100).map(|id| serde_json::json!({
        "output_id": id, "satoshis": 1000, "locking_script": "51", "output_description": "recorded offered coin"
    })).collect::<Vec<_>>()}});
    let text = format(&value);
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), value);
    assert!(text.lines().count() < 110);
}
