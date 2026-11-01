//! The A8 cycle's differential (#556, A8-3): the internalize door's reading
//! of a BEEF on the oracle path and on the `a8-beef-bytes` path (one reading
//! of the bytes for the subject scan, the structure pass and the word pass)
//! under four header contexts. Any difference in a value, an error or a
//! refusal is a crash; a refusal both paths give is a fine answer. Seeded
//! from `corpus/beef_door`.
#![no_main]

use bsv_wallet_toolbox_rs::storage::sqlx::a8_beef_bytes_differential;
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime")
    })
}

fuzz_target!(|data: &[u8]| {
    if let Err(difference) = runtime().block_on(a8_beef_bytes_differential("input", data)) {
        panic!("{difference}");
    }
});
