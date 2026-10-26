//! The Arcade V2 status stream: the SSE client reading the fuzzer's body as
//! its `/events` stream (frames, then each frame's JSON status event), and
//! the frame parser fed the same bytes in two chunks split at the
//! leading byte's offset. No panic is the only assertion.
#![no_main]

use bsv_wallet_toolbox_rs::services::providers::arcade::arcade_reorg_marker;
use bsv_wallet_toolbox_rs::services::providers::arcade::SseFrameParser;
use bsv_wallet_toolbox_rs::{ArcadeSseClient, ArcadeStatusEvent};
use bsv_wallet_toolbox_rs_fuzz::{answer, base, runtime};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((cut, body)) = data.split_first() else {
        return;
    };
    let cut = (*cut as usize).min(body.len());
    let mut parser = SseFrameParser::new();
    let mut frames = parser.push(&body[..cut]);
    frames.extend(parser.push(&body[cut..]));
    for frame in frames.iter().take(16) {
        if let Ok(event) = serde_json::from_str::<ArcadeStatusEvent>(&frame.data) {
            let _ = arcade_reorg_marker(event.extra_info.as_deref());
        }
    }
    answer("/sse", data);
    runtime().block_on(async {
        let Ok(mut client) = ArcadeSseClient::new(format!("{}/sse", base()), "t") else {
            return;
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(4096);
        let _ = client.stream_once(tx).await;
        while rx.try_recv().is_ok() {}
    });
});
