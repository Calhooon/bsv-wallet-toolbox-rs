//! The create door's argument checks live in `crate::wallet::create_args`
//! (#416), compiled under every feature set; the door calls them here.

pub(super) use crate::wallet::create_args::validate_create_action_args;
#[cfg(test)]
use crate::wallet::create_args::{MAX_POSSIBLE_SATOSHIS, MAX_SATOSHIS};

#[cfg(test)]
#[path = "tests/validation_bounds.rs"]
mod bounds_tests;
#[cfg(test)]
#[path = "tests/validation_description.rs"]
mod description_tests;
#[cfg(test)]
#[path = "tests/validation_labels.rs"]
mod label_tests;
#[cfg(test)]
#[path = "tests/validation_nosend_grammar.rs"]
mod nosend_grammar_tests;
#[cfg(test)]
#[path = "tests/validation_nosend.rs"]
mod nosend_tests;
#[cfg(test)]
#[path = "tests/validation.rs"]
mod tests;
