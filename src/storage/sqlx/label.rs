//! Label normalization as the SDK's validateLabel leaves a label
//! (ts-stack@edf6e03 `sdk/src/wallet/validationHelpers.ts:385-387, 411`):
//! `trim().toLowerCase()`. ECMAScript's `String.prototype.trim` strips the
//! WhiteSpace and LineTerminator productions, not Unicode White_Space, which
//! Rust's `str::trim` strips: the two differ on U+FEFF (ECMAScript only) and
//! U+0085 (Rust only), so the doors trim with this set (#379).

/// ECMAScript's WhiteSpace (TAB, VT, FF, ZWNBSP, every Zs) and
/// LineTerminator (LF, CR, LS, PS).
fn is_ecmascript_whitespace(c: char) -> bool {
    matches!(
        c,
        '\u{0009}'
            | '\u{000B}'
            | '\u{000C}'
            | '\u{FEFF}'
            | '\u{000A}'
            | '\u{000D}'
            | '\u{2028}'
            | '\u{2029}'
            // Zs (Unicode 16.0).
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}' | '\u{202F}' | '\u{205F}' | '\u{3000}'
    )
}

/// `String.prototype.trim`.
pub(crate) fn trim_ecmascript(s: &str) -> &str {
    s.trim_matches(is_ecmascript_whitespace)
}

/// The label as validateLabel leaves it: trimmed and lowercased.
pub(crate) fn normalize_label(label: &str) -> String {
    trim_ecmascript(label).to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Across every scalar value the set departs from Unicode White_Space
    /// (`char::is_whitespace`) only at U+0085 and U+FEFF.
    #[test]
    fn set_differs_from_unicode_white_space_at_two_characters() {
        let differing: Vec<char> = (0..=u32::from(char::MAX))
            .filter_map(char::from_u32)
            .filter(|&c| is_ecmascript_whitespace(c) != c.is_whitespace())
            .collect();
        assert_eq!(differing, vec!['\u{0085}', '\u{FEFF}']);
    }

    #[test]
    fn trims_as_ecmascript_trims() {
        assert_eq!(trim_ecmascript("\u{FEFF} a\u{3000}\n"), "a");
        assert_eq!(trim_ecmascript("\u{0085}a\u{0085}"), "\u{0085}a\u{0085}");
        assert_eq!(normalize_label("\t P NoSend \u{2029}"), "p nosend");
    }
}
