default: check

check:
    cargo check --workspace --all-targets --all-features

build:
    cargo build --workspace --all-targets

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
    cargo test --workspace
    cargo test --workspace --all-targets --all-features

check-features:
    cargo test -p another-hatchet-sdk --no-default-features --all-targets
    cargo test -p another-hatchet-sdk --all-features --all-targets

check-examples:
    cargo check -p another-hatchet-sdk-examples --all-targets
    cargo check --locked --manifest-path examples/multi-node/Cargo.toml --all-targets
    cargo test --locked --manifest-path examples/multi-node/Cargo.toml

doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

package-check:
    ./scripts/publish-crates.sh --check-only --allow-dirty

lint: fmt-check clippy

release:
    ./scripts/publish-crates.sh --release

release-prepare:
    ./scripts/publish-crates.sh

publish-dry:
    ./scripts/publish-crates.sh --check-only

publish:
    ./scripts/publish-crates.sh --publish
