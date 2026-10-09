//! Rule 28 witness: the cadence of the stranger's-spend check is named in
//! one place, in the rule's words.
//!
//! The owner's ruling of 2026-10-09 (bsv-stack-lean `NORTH-STAR.md`): the
//! check for a stranger's spend is the irreducible case of Rule 28, run on
//! a cadence the wallet owns and names, until an index of our own watches
//! our outpoints. Until 0.6.0 the cadence was a backoff function in one
//! file and the same pace written as a literal in three more. This test
//! reads the crate's sources: the cadence module exists and carries the
//! rule's words, and no other file writes a number of it.

use std::path::Path;

use bsv_wallet_toolbox_rs::services::cadence::{
    stranger_spend_recheck_minutes, STRANGER_SPEND_LOOKUP_PACE, STRANGER_SPEND_RECHECK_CAP_MINUTES,
    STRANGER_SPEND_RECHECK_FIRST_MINUTES,
};

/// The files that ask about a stranger's spend.
const ASKERS: [&str; 4] = [
    "src/storage/sqlx/locked_inputs.rs",
    "src/storage/sqlx/process_action.rs",
    "src/storage/sqlx/storage_sqlx.rs",
    "src/services/services.rs",
];

#[test]
fn the_cadence_is_named_in_one_place_in_the_rules_words() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cadence = std::fs::read_to_string(manifest.join("src/services/cadence.rs"))
        .expect("src/services/cadence.rs: the one place");
    let words: String = cadence
        .lines()
        .map(|line| {
            line.trim_start_matches("//!")
                .trim_start_matches("///")
                .trim()
        })
        .collect::<Vec<_>>()
        .join(" ");
    for phrase in [
        "a routine poll is the smell of a proof or an index we should hold",
        "the fix is the proof path, never a bigger budget",
        "the irreducible case of Rule 28",
        "a cadence the wallet owns and names",
        "until an index of our own watches our outpoints",
    ] {
        assert!(
            words.contains(phrase),
            "the cadence lacks the rule's words: {phrase}"
        );
    }

    let mut found = Vec::new();
    for file in ASKERS {
        let text = std::fs::read_to_string(manifest.join(file)).expect("read");
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            if code.contains("from_millis(350)")
                || code.contains("1i64 <<")
                || code.contains("BACKOFF_CAP_MINUTES: i64 =")
            {
                found.push(format!("{}:{}: {}", file, n + 1, line.trim()));
            }
        }
    }
    assert!(
        found.is_empty(),
        "a number of the cadence is written outside its one place:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_cadence_is_one_to_sixty_four_minutes_doubling() {
    assert_eq!(STRANGER_SPEND_RECHECK_FIRST_MINUTES, 1);
    assert_eq!(STRANGER_SPEND_RECHECK_CAP_MINUTES, 64);
    assert_eq!(STRANGER_SPEND_LOOKUP_PACE.as_millis(), 350);
    let minutes: Vec<i64> = (0..=9).map(stranger_spend_recheck_minutes).collect();
    assert_eq!(minutes, vec![1, 1, 2, 4, 8, 16, 32, 64, 64, 64]);
    // The names 0.5.0 exported are the same numbers, defined here.
    assert_eq!(
        bsv_wallet_toolbox_rs::locked_input_backoff_minutes(40),
        stranger_spend_recheck_minutes(40)
    );
}
