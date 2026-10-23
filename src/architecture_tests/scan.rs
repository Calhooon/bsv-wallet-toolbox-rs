//! Lexical scanning and production-source discovery for the architecture rules.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Code,
    String,
    Char,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Token<'a> {
    pub(super) text: &'a str,
    pub(super) line: usize,
    pub(super) kind: Kind,
}

// A small lexer keeps braces, comment examples, escaped quotes, raw strings and
// Rust lifetimes from changing which items are production code. Strings retain
// their body for the SQL rule; other rules match only Code tokens.
pub(super) fn lex(source: &str) -> Vec<Token<'_>> {
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

pub(super) fn code(t: &[Token<'_>], i: usize, text: &str) -> bool {
    t.get(i)
        .is_some_and(|t| t.kind == Kind::Code && t.text == text)
}

pub(super) fn sequence(t: &[Token<'_>], i: usize, words: &[&str]) -> bool {
    words
        .iter()
        .enumerate()
        .all(|(n, word)| code(t, i + n, word))
}

pub(super) fn group_end(t: &[Token<'_>], start: usize) -> usize {
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
pub(super) fn production<'a>(t: &[Token<'a>]) -> (Vec<Token<'a>>, Vec<(String, bool)>) {
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

pub(super) struct Source {
    pub(super) path: String,
    pub(super) body: String,
}

pub(super) fn source_files() -> Vec<Source> {
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

pub(super) type Sites = BTreeMap<String, Vec<usize>>;

pub(super) fn scan(detector: fn(&[Token<'_>]) -> Vec<usize>, allowed: fn(&str) -> bool) -> Sites {
    source_files()
        .into_iter()
        .filter(|src| src.path.ends_with(".rs") && !allowed(&src.path))
        .filter_map(|src| {
            let lines = detector(&production(&lex(&src.body)).0);
            (!lines.is_empty()).then_some((src.path, lines))
        })
        .collect()
}

pub(super) fn enforce(rule: &str, sites: Sites, ceilings: &[(&str, usize)]) {
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
        let line = sites
            .get(*path)
            .and_then(|lines| lines.first())
            .unwrap_or(&1);
        eprintln!("{rule}: {path}: {count}/{ceiling}");
        assert_eq!(
            count, *ceiling,
            "{path}:{line}: {rule}: ceiling must equal the current count; lower it when sites disappear"
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
#[should_panic(expected = "src/removed.rs:1: test rule: ceiling must equal the current count")]
fn removed_sites_require_a_lower_ceiling() {
    enforce("test rule", Sites::new(), &[("src/removed.rs", 1)]);
}
