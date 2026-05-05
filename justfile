fmt:
    cargo fmt --all

lint:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings -W clippy::pedantic

build:
    cargo build --workspace --all-targets

dev:
    cargo watch
