//! A `cut` answer, a body cut short in a fixed order (bsv-wallet-toolbox-rs#479).
//!
//! The client must see the status line and some of the body before the
//! connection drops: a body cut short, not a request that got no answer.
//! mockito 1.7 runs a chunked body on a thread of its own and hands each
//! write to hyper over a one-slot channel, and hyper's h1 dispatcher returns
//! a body fault before it flushes what it has buffered. Bare, the scripted
//! body and the fault raced that flush: when hyper took the fault before
//! its write had gone out, the head went with it and the client read
//! "error sending request" where the fixture says "error decoding response
//! body" (once in 1,638 lib tests under load on 2026-10-29; 1 to 10 in
//! 10,000 bare cuts on an idle machine). The order is settled here, not by
//! a clock: padding after the body is more than hyper buffers, so it
//! flushes the head and the body before it polls the stream again and
//! meets the fault only after the client has the head.

/// Frames of spaces a cut sends after its body: more frames than hyper
/// 1.x's server queues before a flush (`MAX_BUF_LIST_BUFFERS`, 16) and more
/// bytes than it holds before one (`DEFAULT_MAX_BUFFER_SIZE`, 417,792), in
/// `proto/h1/io.rs`; either bound forces the flush, whichever write
/// strategy the socket gets.
const PAD_FRAMES: usize = 17;
const PAD_FRAME_LEN: usize = 32 * 1024;

/// A `cut` answer's body: the scripted bytes, the padding, then a fault.
pub(super) fn cut_short(w: &mut dyn std::io::Write, body: &[u8]) -> std::io::Result<()> {
    w.write_all(body)?;
    let pad = vec![b' '; PAD_FRAME_LEN];
    for _ in 0..PAD_FRAMES {
        w.write_all(&pad)?;
    }
    Err(std::io::Error::other("the body cut short"))
}

/// Every cut reaches the client as a head and then a body fault, never as
/// a request with no answer. Counted, not timed: a thousand cuts in a row,
/// each one's head and fault asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ladder_fixtures_a_cut_answer_always_reaches_the_client_as_a_body_cut_short() {
    let mut server = mockito::Server::new_async().await;
    let body = br#"{"index": 0, "txOrId": "aaaaaa"#.to_vec();
    let _mock = server
        .mock("GET", "/cut")
        .with_status(200)
        .with_chunked_body(move |w| cut_short(w, &body))
        .expect_at_least(0)
        .create_async()
        .await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let url = format!("{}/cut", server.url());
    let mut no_head = Vec::new();
    for i in 0..1000 {
        match client.get(&url).send().await {
            Err(e) => no_head.push(format!("cut {i}: {e}")),
            Ok(resp) => {
                assert_eq!(resp.status(), 200, "cut {i}");
                let read = resp.bytes().await;
                assert!(
                    matches!(&read, Err(e) if e.is_body() || e.is_decode()),
                    "cut {i}: the body read {read:?}, not a body cut short"
                );
            }
        }
    }
    assert!(
        no_head.is_empty(),
        "cuts with no head:\n{}",
        no_head.join("\n")
    );
}
