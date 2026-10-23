use std::path::Path;
use std::time::Duration;

use criterion::Criterion;
use serde_json::{json, Value};

pub const METHOD: &str = "individual_wall_time_linear_interpolation_v1";

/// Criterion decides the iteration counts and sampling. Each routine returns
/// only call time; setup, checks and reset happen outside that interval.
pub fn measure(
    criterion: &mut Criterion,
    directory: &Path,
    name: &str,
    mut routine: impl FnMut() -> Duration,
) {
    let mut batches: Vec<Vec<f64>> = Vec::new();
    criterion.bench_function(name, |b| {
        b.iter_custom(|iterations| {
            let mut batch = Vec::with_capacity(iterations as usize);
            let mut total = Duration::ZERO;
            for _ in 0..iterations {
                let elapsed = routine();
                total += elapsed;
                batch.push(elapsed.as_secs_f64() * 1e9);
            }
            batches.push(batch);
            total
        });
    });
    if batches.is_empty() {
        return;
    }

    // The trailing batches correspond exactly to Criterion's measured
    // samples. Earlier batches are warmup and must not affect quantiles.
    let folder = directory.join(name).join("new");
    if std::env::args().any(|arg| arg == "--test") {
        return;
    }
    let sample: Value = serde_json::from_slice(
        &std::fs::read(folder.join("sample.json")).expect("Criterion samples"),
    )
    .expect("sample JSON");
    let iterations = sample["iters"].as_array().expect("sample iterations");
    assert!(batches.len() >= iterations.len());
    let measured = &batches[batches.len() - iterations.len()..];
    for (batch, count) in measured.iter().zip(iterations) {
        assert_eq!(batch.len() as f64, count.as_f64().expect("iteration count"));
    }
    let mut latencies: Vec<f64> = measured.iter().flatten().copied().collect();
    latencies.sort_by(f64::total_cmp);
    assert!(!latencies.is_empty());
    let quantile = |q: f64| {
        let rank = q * (latencies.len() - 1) as f64;
        let low = rank.floor() as usize;
        let high = rank.ceil() as usize;
        latencies[low] + (latencies[high] - latencies[low]) * rank.fract()
    };

    // Criterion provides mean/median estimates of batch averages, but no
    // p99. Add explicitly labelled estimates from individual call times.
    let estimates_path = folder.join("estimates.json");
    let mut estimates: Value =
        serde_json::from_slice(&std::fs::read(&estimates_path).expect("Criterion estimates"))
            .expect("estimates JSON");
    estimates["p50"] = json!({ "point_estimate": quantile(0.50) });
    estimates["p99"] = json!({ "point_estimate": quantile(0.99) });
    estimates["latency_method"] = json!(METHOD);
    estimates["latency_samples"] = json!(latencies.len());
    std::fs::write(
        estimates_path,
        serde_json::to_vec_pretty(&estimates).expect("serialize"),
    )
    .expect("write latency estimates");
    std::fs::write(
        folder.join("latencies.json"),
        serde_json::to_vec(&latencies).expect("serialize"),
    )
    .expect("write individual call samples");
    let run_path = directory.join("door-run.json");
    let mut run: Value =
        serde_json::from_slice(&std::fs::read(&run_path).expect("run manifest")).expect("run JSON");
    run["benchmarks"]
        .as_array_mut()
        .expect("run benchmarks")
        .push(json!(name));
    std::fs::write(
        run_path,
        serde_json::to_vec_pretty(&run).expect("serialize"),
    )
    .expect("record completed benchmark");
}
