.PHONY: test test-all coverage coverage-ci coverage-summary lint fmt doc

test:
	cargo test --lib

test-all:
	cargo test

coverage:
	cargo llvm-cov --lib --html --output-dir coverage/

coverage-ci:
	cargo llvm-cov --lib --lcov --output-path coverage/lcov.info

coverage-summary:
	cargo llvm-cov --lib --summary-only

lint:
	cargo clippy --all-targets --features sqlite -- -D warnings

fmt:
	cargo fmt --all -- --check

doc:
	cargo doc --no-deps --features sqlite
