//! Whether a certificate answers a lookup by attributes, as the reference
//! re-binds an answered certificate to the attributes asked for
//! (`identityUtils.ts` `filterCertificatesByAttributes`): it compares the
//! certificate's decrypted fields, never its encrypted ones.

use std::collections::HashMap;

use super::super::lookup::OverlayCertificate;

/// Fields the reference's all-fields search does not read.
const UNSEARCHABLE_FIELDS: [&str; 2] = ["profilePhoto", "icon"];

/// The reference's English stop words (`IDENTITY_STOP_WORDS`), dropped from
/// both sides of an all-fields search.
const STOP_WORDS: &str = " a about above after again against all am an and any are aren't as at be because been before being below between both but by can't cannot could couldn't did didn't do does doesn't doing don't down during each few for from further had hadn't has hasn't have haven't having he he'd he'll he's her here here's hers herself him himself his how how's i i'd i'll i'm i've if in into is isn't it it's its itself let's me more most mustn't my myself no nor not of off on once only or other ought our ours ourselves out over own same shan't she she'd she'll she's should shouldn't so some such than that that's the their theirs them themselves then there there's these they they'd they'll they're they've this those through to too under until up very was wasn't we we'd we'll we're we've were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't you you'd you'll you're you've your yours yourself yourselves ";

/// `normalizeIdentitySearch`: trimmed, each run of white space one space.
fn normalize(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// `identityFuzzyMatches`: the expected text's tokens found in order,
/// case-insensitively, within one line of the actual text.
fn fuzzy_matches(actual: &str, expected: &str) -> bool {
    let normalized = normalize(expected).to_lowercase();
    let tokens: Vec<&str> = normalized.split(' ').collect();
    let lowered = actual.to_lowercase();
    let lines = lowered.split(['\n', '\r', '\u{2028}', '\u{2029}']);
    lines.into_iter().any(|line| {
        let mut cursor = 0;
        for token in &tokens {
            match line[cursor..].find(token) {
                Some(at) => cursor += at + token.len(),
                None => return false,
            }
        }
        true
    })
}

/// `foldIdentityText`, lower case (the reference also strips combining
/// marks after an NFD decomposition).
fn fold(input: &str) -> String {
    input.to_lowercase()
}

/// A character that separates search terms: white space, or punctuation
/// other than the ASCII apostrophe.
fn separates(c: char) -> bool {
    c != '\''
        && (c.is_whitespace() || c.is_ascii_punctuation() || ('\u{2010}'..='\u{205e}').contains(&c))
}

/// `identityTextTokens`: the terms of `text`, stop words dropped.
fn text_tokens(text: &str) -> Vec<String> {
    text.split(separates)
        .filter(|t| !t.is_empty())
        .filter(|t| !STOP_WORDS.contains(&format!(" {t} ")))
        .map(str::to_string)
        .collect()
}

/// `identityQueryPartMatch`: -1 rejects, 1 is a positive term, 0 neutral.
fn query_part(tokens: &[String], text: &str, raw: &str, phrase: Option<&str>) -> i8 {
    let negative = raw.starts_with('-') && raw.len() > 1;
    if let Some(phrase) = phrase {
        let present = text.contains(phrase);
        if negative == present {
            return -1;
        }
        if negative {
            return 0;
        }
    }
    let source = match phrase {
        Some(p) => p,
        None if negative => &raw[1..],
        None => raw,
    };
    let matched = text_tokens(source).iter().any(|t| tokens.contains(t));
    match (matched, negative) {
        (false, _) => 0,
        (true, true) => -1,
        (true, false) => 1,
    }
}

/// The parts of a query: `-?"phrase"` or a run of characters that are not
/// white space or a quote.
fn query_parts(query: &str) -> Vec<(String, Option<String>)> {
    let chars: Vec<char> = query.chars().collect();
    let mut parts = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let quoted_at = if chars[i] == '"' {
            Some(i)
        } else if chars[i] == '-' && chars.get(i + 1) == Some(&'"') {
            Some(i + 1)
        } else {
            None
        };
        if let Some(q) = quoted_at {
            if let Some(end) = chars[q + 1..].iter().position(|&c| c == '"') {
                let phrase: String = chars[q + 1..q + 1 + end].iter().collect();
                let raw: String = chars[i..q + 2 + end].iter().collect();
                parts.push((raw, Some(phrase)));
                i = q + 2 + end;
                continue;
            }
        }
        if chars[i].is_whitespace() || chars[i] == '"' {
            i += 1;
            continue;
        }
        let start = i;
        while i < chars.len() && !chars[i].is_whitespace() && chars[i] != '"' {
            i += 1;
        }
        parts.push((chars[start..i].iter().collect(), None));
    }
    parts
}

/// `identityTextMatches`: no part rejects and at least one is positive.
fn text_matches(searchable: &str, search: &str) -> bool {
    let text = fold(searchable);
    let tokens = text_tokens(&text);
    let mut positive = false;
    for (raw, phrase) in query_parts(&fold(search)) {
        match query_part(&tokens, &text, &raw, phrase.as_deref()) {
            -1 => return false,
            1 => positive = true,
            _ => {}
        }
    }
    positive
}

/// `identityAnyMatches`: the all-fields search over the searchable fields.
fn any_matches(fields: &HashMap<String, String>, expected: &str) -> bool {
    let normalized = normalize(expected);
    let length = normalized.encode_utf16().count();
    if length < 2 {
        return false;
    }
    let mut names: Vec<&String> = fields.keys().collect();
    names.sort();
    let searchable = names
        .into_iter()
        .filter(|n| !UNSEARCHABLE_FIELDS.contains(&n.as_str()))
        .map(|n| fields[n].as_str())
        .collect::<Vec<_>>()
        .join(" ");
    if length == 2 {
        return fuzzy_matches(&searchable, &normalized);
    }
    text_matches(&searchable, &normalized)
}

/// `identityAttributeMatches`: `userName` exactly, other fields fuzzily.
fn attribute_matches(name: &str, actual: &str, expected: &str) -> bool {
    if name == "userName" {
        return actual == normalize(expected);
    }
    fuzzy_matches(actual, expected)
}

/// Whether a certificate answers a lookup by `attributes`, as the reference
/// checks it: with an `any` attribute, the certificate's searchable
/// decrypted fields satisfy that search; otherwise every attribute whose
/// value is not blank names a decrypted field that matches it, and at least
/// one attribute is not blank.
pub(crate) fn answers_attributes(
    cert: &OverlayCertificate,
    attributes: &HashMap<String, String>,
) -> bool {
    let fields = &cert.decrypted_fields;
    if let Some(any) = attributes.get("any") {
        return any_matches(fields, any);
    }
    let expected: Vec<(&String, &String)> = attributes
        .iter()
        .filter(|(_, v)| !normalize(v).is_empty())
        .collect();
    !expected.is_empty()
        && expected.iter().all(|(name, value)| {
            fields
                .get(name.as_str())
                .is_some_and(|actual| attribute_matches(name, actual, value))
        })
}
