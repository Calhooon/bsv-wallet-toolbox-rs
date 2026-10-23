//! Source boundaries for #56. These are lexical checks, not Rust type analysis.
//! Comments, literals used as examples, and test-only items are distinguished
//! from code. All feature branches are scanned, including disabled backends.
//! Run with `cargo test --lib architecture_tests -- --nocapture` to see ceilings
//! that can fall. Never increase a ceiling just to make a new site pass.
//! Baseline: e9857b8 (2026-10-23). T1/T3: scans only local source; T2/T4/T5:
//! callers cannot grow their authority; T6: each rule has a planted witness.

use std::collections::BTreeSet;

#[cfg(test)]
mod scan;
use scan::*;

// Chaintracks builds its HTTP client in services/providers/chaintracks_client.rs;
// fallback_chain_tracker.rs builds one there too. Neither needs an exception.
// Legacy adapters outside that boundary stay capped until moved in the structure
// phase. #63 owns typed transport errors; #61/#62 must not spread HTTP into SQL.
const HTTP_CEILINGS: &[(&str, usize)] = &[
    ("src/error.rs", 2),               // reqwest error conversion, not a request
    ("src/services/services.rs", 1),   // fiat exchange rate client
    ("src/storage/client/auth.rs", 1), // remote response authentication
    ("src/storage/client/storage_client.rs", 2), // remote storage client
    ("src/wallet/certificate_issuance.rs", 1), // certificate issuer client
    ("src/wallet/lookup.rs", 4),       // overlay lookup client
];

// The current configuration modules. There are NO production env reads at the
// base commit: five reads in create_action.rs and two in sibling_bump_tests.rs
// are test-only diagnostics. #61 moves those diagnostics with their tests.
const CONFIG_MODULES: &[&str] = &["src/services/mod.rs", "src/monitor/config.rs"];

// #61 splits create_action.rs; #62 splits storage_sqlx.rs. Keep each file's
// budget attached to its old path so moving code cannot mint a new allowance.
const PANIC_CEILINGS: &[(&str, usize)] = &[
    ("src/managers/cwi_style_wallet_manager.rs", 1),
    ("src/managers/storage_manager.rs", 11),
    ("src/services/mock.rs", 22),
    ("src/services/providers/arc.rs", 7),
    ("src/services/providers/arcade.rs", 4),
    ("src/services/providers/bitails.rs", 2),
    ("src/services/providers/whatsonchain.rs", 2),
    ("src/services/services.rs", 1),
    ("src/storage/sqlx/internalize_action.rs", 1),
    ("src/storage/sqlx/process_action.rs", 5),
    ("src/storage/sqlx/sync.rs", 1),
];

// #63 replaces string error classification with typed errors end to end.
const ERROR_CEILINGS: &[(&str, usize)] = &[
    ("src/services/providers/arc.rs", 3),
    ("src/services/providers/arcade.rs", 1),
    ("src/services/providers/bitails.rs", 2),
    ("src/services/providers/whatsonchain.rs", 8),
    ("src/storage/client/storage_client.rs", 4),
];

// All these public answers are owned by services (today in traits.rs,
// broadcast_memory.rs and providers/arcade.rs), not by the callers acting on
// them. Reads, match patterns and comparisons are allowed at the boundary.
const SERVICE_ANSWERS: &[&str] = &[
    "UtxoVerdict",
    "BoundSpender",
    "ProviderVerdict",
    "ArcadeVerdict",
    "GetBeefResult",
    "GetRawTxResult",
    "GetMerklePathResult",
    "GetUtxoStatusResult",
    "GetStatusForTxidsResult",
    "GetScriptHashHistoryResult",
    "PostBeefResult",
    "PostTxResultForTxid",
    "UtxoLeads",
    "UtxoLead",
    "UnspentPages",
    "LadderStep",
    "BroadcastStatus",
];

// #62 separates the storage adapters; #63 keeps unknown answers typed.
// The existing no-services fallback may shrink, never spread.
const ANSWER_CEILINGS: &[(&str, usize)] = &[("src/storage/sqlx/poisoned_chain.rs", 1)];

fn http_sites(t: &[Token<'_>]) -> Vec<usize> {
    (0..t.len())
        .filter(|&i| sequence(t, i, &["reqwest", "::"]))
        .map(|i| t[i].line)
        .collect()
}

#[test]
fn provider_requests_stay_in_providers() {
    let sample = "// reqwest::Client\nfn f() { reqwest /* split */ :: Client::new(); }\n#[cfg(test)] mod tests { fn f() { reqwest::get(\"ignored\"); } }\nfn after() { reqwest::get(\"seen\"); }";
    assert_eq!(http_sites(&production(&lex(sample)).0), [2, 4]);
    enforce(
        "HTTP outside services/providers",
        scan(http_sites, |p| p.starts_with("src/services/providers/")),
        HTTP_CEILINGS,
    );
}

fn sql_sites(t: &[Token<'_>]) -> Vec<usize> {
    (0..t.len())
        .filter(|&i| {
            (sequence(t, i, &["sqlx", "::"])
                && t.get(i + 2)
                    .is_some_and(|v| matches!(v.text, "query" | "query_as" | "query_scalar")))
                || (t[i].kind == Kind::String
                    && t[i].text.split_whitespace().next().is_some_and(|s| {
                        matches!(s, "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "CREATE")
                    }))
        })
        .map(|i| t[i].line)
        .collect()
}

#[test]
fn sql_stays_in_storage_sqlx() {
    assert_eq!(
        sql_sites(&lex(
            "sqlx :: query_as::<_, T>(r###\"SELECT x\"###);\nlet s = \"UPDATE things\"; // SELECT"
        )),
        [1, 1, 2]
    );
    let mut sites = scan(sql_sites, |p| p.starts_with("src/storage/sqlx/"));
    for src in source_files()
        .iter()
        .filter(|s| s.path.ends_with(".sql") && !s.path.starts_with("src/storage/sqlx/"))
    {
        sites.insert(src.path.clone(), vec![1]);
    }
    enforce("SQL outside storage/sqlx", sites, &[]);
}

fn env_sites(t: &[Token<'_>]) -> Vec<usize> {
    (0..t.len())
        .filter(|&i| sequence(t, i, &["env", "::", "var"]))
        .map(|i| t[i].line)
        .collect()
}

#[test]
fn environment_reads_stay_in_config() {
    let sample = "#[cfg(all(test, feature = \"x\"))] fn test() { std::env::var(\"ignored\"); }\n#[cfg(any(test, feature = \"x\"))] fn both() { std::env::var(\"seen\"); }\n#[cfg(not(test))] fn prod() { env :: var(\"seen\"); }";
    assert_eq!(env_sites(&production(&lex(sample)).0), [2, 3]);
    enforce(
        "env::var outside config",
        scan(env_sites, |p| CONFIG_MODULES.contains(&p)),
        &[],
    );
}

fn panic_sites(t: &[Token<'_>]) -> Vec<usize> {
    (0..t.len())
        .filter(|&i| {
            (code(t, i, ".") || code(t, i, "::"))
                && (sequence(t, i + 1, &["unwrap", "("]) || sequence(t, i + 1, &["expect", "("]))
        })
        .map(|i| t[i].line)
        .collect()
}

#[test]
fn unwrap_and_expect_do_not_grow() {
    let sample = "const TEXT: &str = r#\".unwrap() /* } */\"#; /* nested /* .expect( */ } */\n#[cfg(test)] mod tests { fn f() { Some(1).unwrap(); } }\nfn after() { Some('{').unwrap(); Some(2).expect(\"why\"); }";
    assert_eq!(panic_sites(&production(&lex(sample)).0), [3, 3]);
    enforce(
        "unwrap/expect outside tests",
        scan(panic_sites, |_| false),
        PANIC_CEILINGS,
    );
}

fn error_word(word: &str) -> bool {
    let word = word.to_ascii_lowercase();
    word == "e"
        || word.contains("err")
        || matches!(
            word.as_str(),
            "message" | "msg" | "reason" | "detail" | "body"
        )
}

fn receiver_start(t: &[Token<'_>], end: usize) -> usize {
    let (mut i, mut depth) = (end, 0usize);
    while i > 0 {
        i -= 1;
        if t[i].kind != Kind::Code {
            continue;
        }
        match t[i].text {
            ")" | "]" => depth += 1,
            "(" | "[" if depth > 0 => depth -= 1,
            ";" | "{" | "}" | "=" | "=>" | "==" | "!=" | "&&" | "||" | "," | "if" | "return"
            | "|" | "("
                if depth == 0 =>
            {
                return i + 1
            }
            _ => (),
        }
    }
    0
}

fn error_expression(t: &[Token<'_>], names: &BTreeSet<&str>) -> bool {
    let named = t
        .iter()
        .any(|v| v.kind == Kind::Code && names.contains(v.text));
    let unrelated_field = (0..t.len().saturating_sub(1))
        .any(|i| code(t, i, ".") && !code(t, i + 2, "(") && !names.contains(t[i + 1].text));
    named && !unrelated_field
}

fn error_sites(t: &[Token<'_>]) -> Vec<usize> {
    let mut tainted: BTreeSet<&str> = t
        .iter()
        .filter(|t| t.kind == Kind::Code && error_word(t.text))
        .map(|t| t.text)
        .collect();
    // A refusal-text classifier's string parameter is also an error message.
    for i in 0..t.len().saturating_sub(3) {
        if sequence(t, i, &["Err", "("]) && code(t, i + 3, ")") {
            tainted.insert(t[i + 2].text);
        }
        if code(t, i + 1, ":") {
            let end = (i + 2..t.len())
                .find(|&j| matches!(t[j].text, "," | ")" | "=" | "{"))
                .unwrap_or(t.len());
            if t[i + 2..end]
                .iter()
                .any(|v| v.kind == Kind::Code && v.text.ends_with("Error"))
            {
                tainted.insert(t[i].text);
            }
        }
        if sequence(t, i, &["fn", "missing_parent_hint", "("]) {
            tainted.insert(t[i + 3].text);
        }
    }
    loop {
        let before = tainted.len();
        for i in 0..t.len() {
            if !code(t, i, "let") {
                continue;
            }
            let name = i + 1 + usize::from(code(t, i + 1, "mut"));
            let end = (name..t.len())
                .find(|&j| code(t, j, ";"))
                .unwrap_or(t.len());
            if let Some(eq) = (name..end).find(|&j| code(t, j, "=")) {
                let rhs = &t[eq + 1..end];
                let simple = !rhs.iter().any(|v| {
                    v.kind == Kind::Code && matches!(v.text, "{" | "}" | "|" | "?" | "=>")
                });
                let transforms_only = (0..rhs.len().saturating_sub(1)).all(|j| {
                    !code(rhs, j, ".")
                        || matches!(
                            rhs[j + 1].text,
                            "to_string"
                                | "to_lowercase"
                                | "to_ascii_lowercase"
                                | "as_str"
                                | "as_deref"
                                | "as_ref"
                                | "clone"
                                | "unwrap_or"
                                | "unwrap_or_default"
                                | "trim"
                                | "trim_start"
                                | "trim_end"
                        )
                });
                if simple && transforms_only && error_expression(rhs, &tainted) {
                    tainted.insert(t[name].text);
                }
            }
        }
        if tainted.len() == before {
            break;
        }
    }
    let mut lines = Vec::new();
    for i in 0..t.len() {
        let method = (code(t, i, "starts_with") || code(t, i, "contains"))
            && i > 0
            && code(t, i - 1, ".")
            && code(t, i + 1, "(");
        let equality = code(t, i, "==");
        if !method && !equality {
            continue;
        }
        let start = receiver_start(t, if method { i - 1 } else { i });
        let left = &t[start..i];
        let right_end = if method {
            group_end(t, i + 1)
        } else {
            (i + 1..t.len())
                .find(|&j| matches!(t[j].text, ";" | "{" | "&&" | "||"))
                .unwrap_or(t.len())
        };
        let right = &t[i + 1..right_end];
        let error = |tokens: &[Token<'_>]| error_expression(tokens, &tainted);
        let literal = |tokens: &[Token<'_>]| tokens.iter().any(|v| v.kind == Kind::String);
        // `needle` is the literal-table iterator used by missing_parent_hint.
        let needle = method && right.iter().any(|v| v.text == "needle") && tainted.contains("text");
        if (error(left) && (literal(right) || needle))
            || (equality && literal(left) && error(right))
        {
            lines.push(t[i].line);
        }
    }
    lines
}

#[test]
fn errors_are_not_classified_by_text() {
    let sample = "fn f(err: Error) {\nlet rendered = err.to_string();\nif rendered.starts_with(\"429\") {}\nif err.message.contains(\"busy\") {}\nif err.to_string() == \"busy\" {}\nif \"busy\" == err.to_string() {}\nif name.starts_with(\"prefix\") {}\n}";
    assert_eq!(error_sites(&lex(sample)), [3, 4, 5, 6]);
    assert_eq!(
        error_sites(&lex(
            "match value { Err(problem) => problem.to_string().contains(\"bad\"), _ => false }"
        )),
        [1]
    );
    enforce(
        "error classified by text",
        scan(error_sites, |_| false),
        ERROR_CEILINGS,
    );
}

fn pattern(t: &[Token<'_>], start: usize, end: usize) -> bool {
    // matches!(value, Variant | Variant) is a consumer, as are match arms.
    for i in 0..start {
        if sequence(t, i, &["matches", "!", "("]) && group_end(t, i + 2) > start {
            let mut j = i + 3;
            while j < start {
                if code(t, j, ",") {
                    return true;
                }
                j = if code(t, j, "(") || code(t, j, "[") {
                    group_end(t, j)
                } else {
                    j + 1
                };
            }
        }
    }
    let mut i = end;
    while i < t.len() {
        if code(t, i, "=>") {
            return true;
        }
        if code(t, i, "=") {
            return t[..start]
                .iter()
                .rev()
                .take_while(|v| !matches!(v.text, "=" | ";" | "{"))
                .any(|v| v.text == "let");
        }
        if t[i].kind == Kind::Code && matches!(t[i].text, "," | ";" | "=" | "}") {
            break;
        }
        i = if code(t, i, "{") || code(t, i, "(") {
            group_end(t, i)
        } else {
            i + 1
        };
    }
    false
}

fn answer_sites(t: &[Token<'_>]) -> Vec<usize> {
    (0..t.len())
        .filter(|&i| {
            if t[i].kind != Kind::Code || !SERVICE_ANSWERS.contains(&t[i].text) {
                return false;
            }
            if i > 0 && (code(t, i - 1, "struct") || code(t, i - 1, "enum") || code(t, i - 1, "->"))
            {
                return false;
            }
            let end = if code(t, i + 1, "{") {
                i + 1
            } else if code(t, i + 1, "::")
                && t.get(i + 2).is_some_and(|v| {
                    v.text.starts_with(char::is_uppercase) || matches!(v.text, "new" | "default")
                })
            {
                i + 3
            } else {
                return false;
            };
            let compared = (i > 0 && (code(t, i - 1, "==") || code(t, i - 1, "!=")))
                || code(t, end, "==")
                || code(t, end, "!=");
            !compared && !pattern(t, i, end)
        })
        .map(|i| t[i].line)
        .collect()
}

#[test]
fn service_answers_are_only_built_by_services() {
    let sample = "match v { UtxoVerdict::Spent => (), UtxoVerdict::Unknown => () }\nlet ok = matches!(v, UtxoVerdict::Spent | UtxoVerdict::Unknown);\nif v == UtxoVerdict::SpentHint {}\nlet forged = UtxoVerdict::Spent;\nlet bound = BoundSpender { txid, confirmed: true };\nlet result = GetRawTxResult { ..old };";
    assert_eq!(answer_sites(&lex(sample)), [4, 5, 6]);
    enforce(
        "service answer constructed outside services",
        scan(answer_sites, |p| p.starts_with("src/services/")),
        ANSWER_CEILINGS,
    );
}
