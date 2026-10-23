//! Source boundaries for #56. These are lexical checks, not Rust type analysis.
//! Comments, literals used as examples, and test-only items are distinguished
//! from code. All feature branches are scanned, including disabled backends.
//! Run with `cargo test --lib architecture_tests -- --nocapture` to see ceilings
//! that can fall. Never increase a ceiling just to make a new site pass.
//! Baseline: e9857b8 (2026-10-23). T1/T3: scans only local source; T2/T4/T5:
//! callers cannot grow their authority; T6: each rule has a planted witness.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Code,
    String,
    Char,
}

#[derive(Clone, Copy, Debug)]
struct Token<'a> {
    text: &'a str,
    line: usize,
    kind: Kind,
}

// A small lexer keeps braces, comment examples, escaped quotes, raw strings and
// Rust lifetimes from changing which items are production code. Strings retain
// their body for the SQL rule; other rules match only Code tokens.
fn lex(source: &str) -> Vec<Token<'_>> {
    let b = source.as_bytes();
    let (mut i, mut line) = (0, 1);
    let mut out = Vec::new();
    while i < b.len() {
        let start = i;
        let at_line = line;
        if b[i].is_ascii_whitespace() {
            i += 1;
        } else if b[i..].starts_with(b"//") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < b.len() && depth != 0 {
                if b[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if b[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else {
            let raw = if b[i..].starts_with(b"br") {
                i + 2
            } else if b[i] == b'r' {
                i + 1
            } else {
                i
            };
            let mut quote = raw;
            while quote < b.len() && b[quote] == b'#' {
                quote += 1;
            }
            if raw != i && quote < b.len() && b[quote] == b'"' {
                let hashes = quote - raw;
                i = quote + 1;
                let body = i;
                while i < b.len()
                    && !(b[i] == b'"' && b.get(i + 1..i + 1 + hashes) == Some(&b[raw..quote]))
                {
                    i += 1;
                }
                out.push(Token {
                    text: &source[body..i],
                    line: at_line,
                    kind: Kind::String,
                });
                i = (i + 1 + hashes).min(b.len());
            } else if b[i] == b'"' || b[i..].starts_with(b"b\"") {
                if b[i] == b'b' {
                    i += 1;
                }
                i += 1;
                let body = i;
                while i < b.len() && b[i] != b'"' {
                    i += if b[i] == b'\\' && i + 1 < b.len() {
                        2
                    } else {
                        1
                    };
                }
                out.push(Token {
                    text: &source[body..i],
                    line: at_line,
                    kind: Kind::String,
                });
                i = (i + 1).min(b.len());
            } else if b[i] == b'\''
                && (b.get(i + 2) == Some(&b'\'') || b.get(i + 1) == Some(&b'\\'))
            {
                i += 1;
                while i < b.len() && b[i] != b'\'' {
                    i += if b[i] == b'\\' && i + 1 < b.len() {
                        2
                    } else {
                        1
                    };
                }
                i = (i + 1).min(b.len());
                out.push(Token {
                    text: &source[start..i],
                    line: at_line,
                    kind: Kind::Char,
                });
            } else {
                if b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] >= 128 {
                    i += 1;
                    while i < b.len()
                        && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] >= 128)
                    {
                        i += 1;
                    }
                } else {
                    i += if [b"::", b"==", b"!=", b"=>", b"&&", b"||", b"->"]
                        .iter()
                        .any(|pair| b[start..].starts_with(*pair))
                    {
                        2
                    } else {
                        1
                    };
                }
                out.push(Token {
                    text: &source[start..i],
                    line: at_line,
                    kind: Kind::Code,
                });
            }
        }
        line += b[start..i].iter().filter(|&&c| c == b'\n').count();
    }
    out
}

fn code(t: &[Token<'_>], i: usize, text: &str) -> bool {
    t.get(i)
        .is_some_and(|t| t.kind == Kind::Code && t.text == text)
}

fn sequence(t: &[Token<'_>], i: usize, words: &[&str]) -> bool {
    words
        .iter()
        .enumerate()
        .all(|(n, word)| code(t, i + n, word))
}

fn group_end(t: &[Token<'_>], start: usize) -> usize {
    let close = match t[start].text {
        "(" => ")",
        "[" => "]",
        "{" => "}",
        _ => return start + 1,
    };
    let mut i = start + 1;
    while i < t.len() {
        if code(t, i, close) {
            return i + 1;
        }
        if t[i].kind == Kind::Code && matches!(t[i].text, "(" | "[" | "{") {
            i = group_end(t, i);
        } else {
            i += 1;
        }
    }
    panic!("unclosed source group on line {}", t[start].line);
}

// True only if this cfg cannot compile without `test`. In particular,
// any(test, feature = "...") and not(test) must still be scanned.
fn requires_test(t: &[Token<'_>]) -> bool {
    if t.len() == 1 {
        return code(t, 0, "test");
    }
    if !code(t, 1, "(") {
        return false;
    }
    let mut args = Vec::new();
    let (mut start, mut i) = (2, 2);
    while i + 1 < t.len() {
        if code(t, i, ",") {
            args.push(requires_test(&t[start..i]));
            start = i + 1;
        }
        i = if code(t, i, "(") {
            group_end(t, i)
        } else {
            i + 1
        };
    }
    if start < t.len() - 1 {
        args.push(requires_test(&t[start..t.len() - 1]));
    }
    match t[0].text {
        "all" => args.contains(&true),
        "any" => !args.is_empty() && args.iter().all(|v| *v),
        _ => false,
    }
}

fn item_end(t: &[Token<'_>], mut i: usize) -> usize {
    while i < t.len() {
        if code(t, i, ";") {
            return i + 1;
        }
        if code(t, i, "{") {
            return group_end(t, i);
        }
        i = if code(t, i, "(") || code(t, i, "[") {
            group_end(t, i)
        } else {
            i + 1
        };
    }
    i
}

// Return production tokens and the names of out-of-line test modules. Inline
// test modules, functions, impls and statics are removed as complete items, so
// production items *after* a test module are still scanned.
fn production<'a>(t: &[Token<'a>]) -> (Vec<Token<'a>>, Vec<(String, bool)>) {
    let (mut out, mut modules, mut i) = (Vec::new(), Vec::new(), 0);
    while i < t.len() {
        if sequence(t, i, &["#", "["]) {
            let start = i;
            let mut test_only = false;
            let mut explicit_path = None;
            while sequence(t, i, &["#", "["]) {
                let end = group_end(t, i + 1);
                test_only |= sequence(t, i + 2, &["cfg", "("]) && requires_test(&t[i + 4..end - 2]);
                test_only |= code(t, i + 2, "test") || sequence(t, i + 2, &["tokio", "::", "test"]);
                if sequence(t, i + 2, &["path", "="]) {
                    explicit_path = Some(t[i + 4].text.to_owned());
                }
                i = end;
            }
            if test_only {
                let end = item_end(t, i);
                for j in i..end.saturating_sub(2) {
                    if code(t, j, "mod") && code(t, j + 2, ";") {
                        modules.push(match &explicit_path {
                            Some(path) => (path.clone(), true),
                            None => (t[j + 1].text.to_owned(), false),
                        });
                    }
                }
                i = end;
            } else {
                out.extend_from_slice(&t[start..i]);
            }
        } else {
            out.push(t[i]);
            i += 1;
        }
    }
    (out, modules)
}

struct Source {
    path: String,
    body: String,
}

fn source_files() -> Vec<Source> {
    fn walk(root: &Path, paths: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(root).unwrap_or_else(|e| panic!("{}: {e}", root.display())) {
            let path = entry.expect("source directory entry").path();
            if path.is_dir() {
                walk(&path, paths);
            } else if path
                .extension()
                .is_some_and(|ext| ext == "rs" || ext == "sql")
            {
                paths.push(path);
            }
        }
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut paths = Vec::new();
    walk(&root.join("src"), &mut paths);
    paths.sort();
    let sources: Vec<_> = paths
        .iter()
        .map(|path| Source {
            path: path
                .strip_prefix(root)
                .expect("under manifest")
                .to_string_lossy()
                .replace('\\', "/"),
            body: fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display())),
        })
        .collect();
    let mut tests = BTreeSet::new();
    for src in &sources {
        if !src.path.ends_with(".rs") {
            continue;
        }
        for (module, explicit) in production(&lex(&src.body)).1 {
            let path = Path::new(&src.path);
            let parent = path.parent().expect("source parent");
            if explicit {
                tests.insert(parent.join(module).to_string_lossy().into_owned());
                continue;
            }
            let dir = if matches!(
                path.file_name().and_then(|p| p.to_str()),
                Some("lib.rs" | "mod.rs")
            ) {
                parent.to_path_buf()
            } else {
                parent.join(path.file_stem().expect("source stem"))
            };
            tests.insert(format!("{}/{module}.rs", dir.display()));
            tests.insert(format!("{}/{module}/", dir.display()));
        }
    }
    sources
        .into_iter()
        .filter(|src| {
            !tests
                .iter()
                .any(|p| src.path == *p || (p.ends_with('/') && src.path.starts_with(p)))
        })
        .collect()
}

type Sites = BTreeMap<String, Vec<usize>>;

fn scan(detector: fn(&[Token<'_>]) -> Vec<usize>, allowed: fn(&str) -> bool) -> Sites {
    source_files()
        .into_iter()
        .filter(|src| src.path.ends_with(".rs") && !allowed(&src.path))
        .filter_map(|src| {
            let lines = detector(&production(&lex(&src.body)).0);
            (!lines.is_empty()).then_some((src.path, lines))
        })
        .collect()
}

fn enforce(rule: &str, sites: Sites, ceilings: &[(&str, usize)]) {
    let mut failures = Vec::new();
    for (path, lines) in &sites {
        let ceiling = ceilings
            .iter()
            .find(|(p, _)| p == path)
            .map_or(0, |(_, n)| *n);
        if lines.len() > ceiling {
            for line in lines {
                failures.push(format!(
                    "{path}:{line}: {rule} ({} sites, ceiling {ceiling})",
                    lines.len()
                ));
            }
        }
    }
    for (path, ceiling) in ceilings {
        let count = sites.get(*path).map_or(0, Vec::len);
        eprintln!(
            "{rule}: {path}: {count}/{ceiling}{}",
            if count < *ceiling {
                "; lower this ceiling"
            } else {
                ""
            }
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

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
