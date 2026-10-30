//! The reference's attribute matcher, ts-stack edf6e03
//! `packages/wallet/wallet-toolbox/src/utility/identityUtils.ts:140-280`, ported
//! helper by helper in the reference's branch order (#474). The overlay's
//! attribute search is re-run on the decrypted fields it answered, so a
//! certificate the overlay matched on other grounds is dropped.
//!
//! Strings are JavaScript strings in the reference: a `length` is UTF-16 code
//! units, `\s` is ECMAScript's `WhiteSpace` and `LineTerminator` set (the set
//! `trim_ecmascript` strips), and an object's own keys enumerate array indices
//! in ascending order before the other keys in insertion order.

use std::sync::LazyLock;

use indexmap::IndexMap;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::wallet::label::trim_ecmascript;
use crate::{Error, Result};

/// `\s` of an ECMAScript regular expression: the set `String.prototype.trim`
/// strips, read from the one place this crate defines it.
fn is_ecmascript_space(c: char) -> bool {
    trim_ecmascript(c.encode_utf8(&mut [0; 4])).is_empty()
}

/// `String.prototype.length`: UTF-16 code units.
fn js_length(s: &str) -> usize {
    s.encode_utf16().count()
}

/// TS `:140-142` `normalizeIdentitySearch`: `trim()`, then every `\s+` run
/// replaced by one space.
pub(super) fn normalize_identity_search(input: &str) -> String {
    let mut normalized = String::with_capacity(input.len());
    let mut in_space = false;
    for c in trim_ecmascript(input).chars() {
        if is_ecmascript_space(c) {
            if !in_space {
                normalized.push(' ');
            }
            in_space = true;
        } else {
            normalized.push(c);
            in_space = false;
        }
    }
    normalized
}

/// `actual.split(/\r\n?|\n| | /)` (TS `:147`).
fn identity_lines(actual: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = actual.char_indices().peekable();
    while let Some((at, c)) = chars.next() {
        let end = match c {
            '\r' => match chars.peek() {
                Some(&(next, '\n')) => {
                    chars.next();
                    next + 1
                }
                _ => at + 1,
            },
            '\n' => at + 1,
            '\u{2028}' | '\u{2029}' => at + 3,
            _ => continue,
        };
        lines.push(&actual[start..at]);
        start = end;
    }
    lines.push(&actual[start..]);
    lines
}

/// `Canonicalize` of a `RegExp` with the `i` flag and no `u` flag (ECMA-262
/// 22.2.2.7.3): each code unit through `toUpperCase`, kept as it is when the
/// uppercase is more than one code unit or maps a non-ASCII unit to ASCII. A
/// character outside the BMP is two surrogate units, each its own canonical
/// form, so it matches only itself.
fn legacy_canonicalize(c: char) -> char {
    if u32::from(c) > 0xFFFF {
        return c;
    }
    let mut upper = c.to_uppercase();
    match (upper.next(), upper.next()) {
        (Some(u), None)
            if u32::from(u) <= 0xFFFF && !(u32::from(c) >= 128 && u32::from(u) < 128) =>
        {
            u
        }
        _ => c,
    }
}

/// The end of the leftmost case-insensitive match of `literal` in `line` at or
/// after `cursor` (TS `:152-156`: an escaped literal under `gi` from `lastIndex`).
fn find_literal(line: &[char], literal: &[char], cursor: usize) -> Option<usize> {
    let last = line.len().checked_sub(literal.len())?;
    (cursor..=last)
        .find(|&start| {
            line[start..start + literal.len()]
                .iter()
                .zip(literal)
                .all(|(&a, &b)| legacy_canonicalize(a) == legacy_canonicalize(b))
        })
        .map(|start| start + literal.len())
}

/// TS `:145-163` `identityFuzzyMatches`: the expected string's tokens matched
/// in order as literals within one line of the actual value. Matching runs on
/// characters: a literal from a valid string never begins at a low surrogate,
/// so a code-unit match is a character match.
pub(super) fn identity_fuzzy_matches(actual: &str, expected: &str) -> bool {
    let tokens: Vec<Vec<char>> = normalize_identity_search(expected)
        .split(' ')
        .map(|token| token.chars().collect())
        .collect();
    for line in identity_lines(actual) {
        let line: Vec<char> = line.chars().collect();
        let mut cursor = 0;
        let mut token_index = 0;
        while token_index < tokens.len() {
            let Some(end) = find_literal(&line, &tokens[token_index], cursor) else {
                break;
            };
            cursor = end;
            token_index += 1;
        }
        if token_index == tokens.len() {
            return true;
        }
    }
    false
}

/// TS `:166-167` `IDENTITY_STOP_WORDS`, byte for byte.
const IDENTITY_STOP_WORDS: &str = " a about above after again against all am an and any are aren't as at be because been before being below between both but by can't cannot could couldn't did didn't do does doesn't doing don't down during each few for from further had hadn't has hasn't have haven't having he he'd he'll he's her here here's hers herself him himself his how how's i i'd i'll i'm i've if in into is isn't it it's its itself let's me more most mustn't my myself no nor not of off on once only or other ought our ours ourselves out over own same shan't she she'd she'll she's should shouldn't so some such than that that's the their theirs them themselves then there there's these they they'd they'll they're they've this those through to too under until up very was wasn't we we'd we'll we're we've were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't you you'd you'll you're you've your yours yourself yourselves ";

/// TS `:171-172`: runs outside the five classes, the ASCII apostrophe kept.
static IDENTITY_TOKEN: LazyLock<std::result::Result<Regex, regex::Error>> = LazyLock::new(|| {
    Regex::new(
        r"(?:[^\p{Dash}\p{Pattern_Syntax}\p{Quotation_Mark}\p{Terminal_Punctuation}\p{White_Space}]|')+",
    )
});

/// A pattern built once; a build failure is the crate's internal fault.
fn pattern(
    built: &'static LazyLock<std::result::Result<Regex, regex::Error>>,
) -> Result<&'static Regex> {
    built
        .as_ref()
        .map_err(|e| Error::Internal(format!("identity search pattern: {e}")))
}

/// TS `:169-174` `identityTextTokens`: the tokens, stop words dropped.
fn identity_text_tokens(text: &str) -> Result<Vec<&str>> {
    Ok(pattern(&IDENTITY_TOKEN)?
        .find_iter(text)
        .map(|token| token.as_str())
        .filter(|token| !IDENTITY_STOP_WORDS.contains(&format!(" {token} ")))
        .collect())
}

/// TS `:177-188` `identityQueryPartMatch`: -1 rejects the result, 1 records a
/// positive indexed term, 0 is neutral.
fn identity_query_part_match(
    tokens: &[&str],
    text: &str,
    raw: &str,
    phrase: Option<&str>,
) -> Result<i8> {
    let negative = raw.starts_with('-') && js_length(raw) > 1;
    if let Some(phrase) = phrase {
        let present = contains_text(text, phrase);
        if if negative { present } else { !present } {
            return Ok(-1);
        }
        if negative {
            return Ok(0);
        }
    }
    let terms = identity_text_tokens(phrase.unwrap_or(if negative { &raw[1..] } else { raw }))?;
    let matched = terms.iter().any(|term| tokens.contains(term));
    if !matched {
        return Ok(0);
    }
    Ok(if negative { -1 } else { 1 })
}

/// The successive matches of `/-?"([^"]*)"|[^\s"]+/g` (TS `:199`): each part
/// with its phrase group when the quoted branch matched.
fn identity_query_parts(query: &str) -> Vec<(&str, Option<&str>)> {
    let mut parts = Vec::new();
    let mut at = 0;
    while at < query.len() {
        let rest = &query[at..];
        let quoted = rest.strip_prefix('-').unwrap_or(rest);
        if let Some(inner) = quoted.strip_prefix('"') {
            if let Some(close) = inner.find('"') {
                let end = at + (rest.len() - inner.len()) + close + 1;
                parts.push((&query[at..end], Some(&inner[..close])));
                at = end;
                continue;
            }
        }
        let run = rest
            .find(|c| c == '"' || is_ecmascript_space(c))
            .unwrap_or(rest.len());
        if run > 0 {
            parts.push((&rest[..run], None));
            at += run;
        } else {
            at += rest.chars().next().map_or(1, char::len_utf8);
        }
    }
    parts
}

/// TS `:195-209` `identityTextMatches`: complete tokens, stop words and exact
/// phrases on both sides of the fold.
fn identity_text_matches(searchable: &str, search: &str) -> Result<bool> {
    let text = fold_identity_text(searchable)?;
    let query = fold_identity_text(search)?;
    let tokens = identity_text_tokens(&text)?;
    let mut positive_match = false;
    for (raw, phrase) in identity_query_parts(&query) {
        match identity_query_part_match(&tokens, &text, raw, phrase)? {
            -1 => return Ok(false),
            1 => positive_match = true,
            _ => {}
        }
    }
    Ok(positive_match)
}

/// TS `:212-216` `identityAttributeMatches`: `userName` exactly, other named
/// fields fuzzily. The decrypted fields are strings here, so the reference's
/// non-string branch has no input.
pub(super) fn identity_attribute_matches(field_name: &str, actual: &str, expected: &str) -> bool {
    if field_name == "userName" {
        return actual == normalize_identity_search(expected);
    }
    identity_fuzzy_matches(actual, expected)
}

static NONSPACING_MARK: LazyLock<std::result::Result<Regex, regex::Error>> =
    LazyLock::new(|| Regex::new(r"\p{Mn}"));

/// TS `:218-223` `foldIdentityText`: NFD, every `\p{Mn}` removed, lowercase.
pub(super) fn fold_identity_text(input: &str) -> Result<String> {
    let decomposed: String = input.nfd().collect();
    Ok(pattern(&NONSPACING_MARK)?
        .replace_all(&decomposed, "")
        .to_lowercase())
}

/// TS `:225-227` `containsText`.
fn contains_text(haystack: &str, needle: &str) -> bool {
    haystack.contains(needle)
}

/// TS `:229` `UNSEARCHABLE_IDENTITY_FIELDS`.
const UNSEARCHABLE_IDENTITY_FIELDS: [&str; 2] = ["profilePhoto", "icon"];

/// An ECMAScript array index: a canonical decimal below 2^32 - 1.
fn is_array_index(key: &str) -> bool {
    (key == "0" || (!key.starts_with('0') && !key.is_empty()))
        && key.bytes().all(|b| b.is_ascii_digit())
        && key.parse::<u64>().is_ok_and(|n| n < u64::from(u32::MAX))
}

/// `Object.entries` order: array indices ascending, then insertion order.
fn own_entries(fields: &IndexMap<String, String>) -> Vec<(&str, &str)> {
    let mut indices: Vec<_> = fields
        .iter()
        .filter(|(key, _)| is_array_index(key))
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    indices.sort_by_key(|(key, _)| key.parse::<u64>().unwrap_or(0));
    let named = fields
        .iter()
        .filter(|(key, _)| !is_array_index(key))
        .map(|(key, value)| (key.as_str(), value.as_str()));
    indices.into_iter().chain(named).collect()
}

/// TS `:235-244` `identityAnyMatches`: `any` is the overlay's all-fields search.
pub(super) fn identity_any_matches(
    fields: &IndexMap<String, String>,
    expected: &str,
) -> Result<bool> {
    let normalized = normalize_identity_search(expected);
    let length = js_length(&normalized);
    if length < 2 {
        return Ok(false);
    }
    let searchable = own_entries(fields)
        .into_iter()
        .filter(|(key, _)| !UNSEARCHABLE_IDENTITY_FIELDS.contains(key))
        .map(|(_, value)| value)
        .collect::<Vec<_>>()
        .join(" ");
    if length == 2 {
        return Ok(identity_fuzzy_matches(&searchable, &normalized));
    }
    identity_text_matches(&searchable, &normalized)
}

/// TS `:257-265` `usableIdentityAttributes`: the pairs whose value normalizes
/// to a non-empty search. The typed map holds strings only, so the
/// reference's `undefined` (a non-string value) has no input.
pub(super) fn usable_identity_attributes(
    attributes: &IndexMap<String, String>,
) -> Vec<(&str, &str)> {
    own_entries(attributes)
        .into_iter()
        .filter(|(_, value)| !normalize_identity_search(value).is_empty())
        .collect()
}

/// TS `:267-280` `certificateMatchesAttributes` on the decrypted fields. An
/// absent `decryptedFields` is the empty map, which no pair and no `any`
/// search matches, as the reference's `false`.
pub(super) fn certificate_matches_attributes(
    fields: &IndexMap<String, String>,
    any_search: Option<&str>,
    expected: &[(&str, &str)],
) -> Result<bool> {
    if let Some(any_search) = any_search {
        return identity_any_matches(fields, any_search);
    }
    Ok(expected.iter().all(|(field_name, value)| {
        fields
            .get(*field_name)
            .is_some_and(|actual| identity_attribute_matches(field_name, actual, value))
    }))
}
