use super::RandomSource;

#[test]
fn seeded_sources_repeat_the_stream_and_advance() {
    let left = RandomSource::new(Some([42; 32]));
    let right = RandomSource::new(Some([42; 32]));
    let mut previous = [0; 64];
    for index in 0..3 {
        let mut a = [0; 64];
        let mut b = [0; 64];
        left.fill_bytes(&mut a);
        right.fill_bytes(&mut b);
        assert_eq!(a, b);
        if index > 0 {
            assert_ne!(a, previous);
        }
        previous = a;
    }
}

#[test]
fn distinct_seeds_and_os_entropy_give_distinct_streams() {
    let sources = [
        RandomSource::new(Some([1; 32])),
        RandomSource::new(Some([2; 32])),
        RandomSource::new(None),
        RandomSource::new(None),
    ];
    let mut outputs = [[0; 32]; 4];
    for (source, output) in sources.iter().zip(&mut outputs) {
        source.fill_bytes(output);
    }
    for (index, output) in outputs.iter().enumerate() {
        assert!(!outputs[..index].contains(output));
    }
}
