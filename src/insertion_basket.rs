//! The basket insertion's basket, read as the reference reads it.

/// The refusal of the default basket as a basket insertion's basket, the
/// reference's `WERR_INVALID_PARAMETER('insertionRemittance.basket', 'a
/// non-default basket')` (wallet-toolbox `signer/methods/internalizeAction.ts`
/// `setupBasketInsertionForOutput`, storage `internalizeAction.ts` `getBasket`
/// and `classifyRequestedOutput`, ts-stack 4d0ea63c8).
pub(crate) const DEFAULT_BASKET_INSERTION_REFUSAL: &str =
    "The insertionRemittance.basket parameter must be a non-default basket";

/// Whether a basket insertion's `basket` names the default basket as the
/// reference reads it: the reference's validator (`validateIdentifier` in
/// `@bsv/sdk`'s `validationHelpers.ts`) trims the name with JavaScript's
/// `trim` and lowercases it before the comparison with `default`, so every
/// spelling that trims and lowercases to `default` is the default basket.
/// JavaScript's `trim` strips U+FEFF and keeps U+0085, where Rust's
/// `char::is_whitespace` does the reverse.
pub(crate) fn names_default_basket(basket: &str) -> bool {
    let js_space = |c: char| (c.is_whitespace() && c != '\u{85}') || c == '\u{FEFF}';
    basket.trim_matches(js_space).to_lowercase() == "default"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_spelling_the_reference_lowercases_and_trims_to_default_names_it() {
        for name in [
            "default",
            "Default",
            "DEFAULT",
            " default",
            "default ",
            "\tdefault\n",
            "\u{FEFF}default",
            "\u{A0}DeFaUlT\u{3000}",
        ] {
            assert!(names_default_basket(name), "{name:?}");
        }
        for name in ["defaults", "de fault", "\u{85}default", "tokens", ""] {
            assert!(!names_default_basket(name), "{name:?}");
        }
    }
}
