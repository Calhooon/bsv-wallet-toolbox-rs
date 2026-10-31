//! Label normalization as the SDK's `validateLabel` leaves a label
//! (ts-stack@edf6e03 `sdk/src/wallet/validationHelpers.ts:385-387, 411`):
//! `trim().toLowerCase()`. ECMAScript's `String.prototype.trim` strips the
//! `WhiteSpace` and `LineTerminator` productions, not Unicode `White_Space`, which
//! Rust's `str::trim` strips: the two differ on U+FEFF (ECMAScript only) and
//! U+0085 (Rust only), so the doors trim with this set (#379).

/// ECMAScript's `WhiteSpace` (TAB, VT, FF, ZWNBSP, every Zs) and
/// `LineTerminator` (LF, CR, LS, PS).
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

/// ECMAScript's `String.prototype.trim`: strips the `WhiteSpace` and
/// `LineTerminator` code points from both ends.
///
/// It differs from Rust's [`str::trim`] (Unicode `White_Space`) on two code
/// points: U+FEFF (ZERO WIDTH NO-BREAK SPACE) is stripped here and kept by
/// `str::trim`; U+0085 (NEXT LINE) is kept here and stripped by `str::trim`.
///
/// ```
/// use bsv_wallet_toolbox_rs::wallet::trim_ecmascript;
/// assert_eq!(trim_ecmascript("\u{feff}a\u{feff}"), "a");
/// assert_eq!("\u{feff}a\u{feff}".trim(), "\u{feff}a\u{feff}");
/// assert_eq!(trim_ecmascript("\u{0085}a\u{0085}"), "\u{0085}a\u{0085}");
/// assert_eq!("\u{0085}a\u{0085}".trim(), "a");
/// ```
#[must_use]
pub fn trim_ecmascript(s: &str) -> &str {
    s.trim_matches(is_ecmascript_whitespace)
}

/// A label as the SDK's `validateLabel` leaves it, the one definition of the
/// reference's label rule for every host: `s.trim().toLowerCase()` with
/// ECMAScript's trim ([`trim_ecmascript`]), then lowercase
/// (ts-stack@edf6e03 `sdk/src/wallet/validationHelpers.ts:385-387`, which
/// delegates to `validateIdentifier` at `:409-418`, the normalization at
/// `:411`). The wallet's doors store and filter labels in this form; the
/// byte bounds (1 to 300) are the doors' own check, not this function's.
///
/// ```
/// use bsv_wallet_toolbox_rs::wallet::normalize_label;
/// assert_eq!(normalize_label("\u{feff} Spend-Gate:X\t"), "spend-gate:x");
/// assert_eq!(normalize_label("\u{0085}a"), "\u{0085}a");
/// ```
#[must_use]
pub fn normalize_label(label: &str) -> String {
    trim_ecmascript(label).to_lowercase()
}

/// BRC-177's module prefix (ts-stack@edf6e03 `brc177NoSendExpiry.ts:5`).
const BRC177_NO_SEND_MODULE_PREFIX: &str = "p nosend ";

/// Whether a label, as `normalize_label` leaves it, names BRC-177's module:
/// the `startsWith` of `hasBrc177NoSendExpiryLabel`
/// (ts-stack@edf6e03 `brc177NoSendExpiry.ts:119`). The create door counts
/// these labels ahead of its grammar; the internalize door refuses any.
pub(crate) fn is_brc177_no_send_label(normalized: &str) -> bool {
    normalized.starts_with(BRC177_NO_SEND_MODULE_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Across every scalar value the set departs from Unicode `White_Space`
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

    /// The CLI's spend gate (bsv-wallet-cli#376) reads the stored form
    /// through this one definition: a leading U+FEFF and a trailing space
    /// trimmed, the case folded.
    #[test]
    fn normalize_label_gives_the_stored_form() {
        assert_eq!(normalize_label("\u{feff}Spend-Gate:x "), "spend-gate:x");
    }
}
