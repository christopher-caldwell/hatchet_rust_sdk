#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    "Usage:" \
    "  scripts/publish-crates.sh --release      # prepare, commit, publish, and tag" \
    "  scripts/publish-crates.sh                # prepare release metadata + validate" \
    "  scripts/publish-crates.sh --check-only   # validate without changing metadata" \
    "  scripts/publish-crates.sh --publish      # publish committed release metadata" \
    "" \
    "Options:" \
    "  --release      Run the complete guarded release from a clean branch." \
    "  --publish      Publish all three crates in dependency order." \
    "  --check-only   Skip release-plz and run the full preflight." \
    "  --yes          Skip the permanent-publish confirmation." \
    "  --allow-dirty  Permit check-only validation of local changes." \
    "  -h, --help     Show this help."
}

mode="prepare"
allow_dirty=false
assume_yes=false

for arg in "$@"; do
  case "$arg" in
    --release) mode="release" ;;
    --publish) mode="publish" ;;
    --check-only) mode="check-only" ;;
    --yes) assume_yes=true ;;
    --allow-dirty) allow_dirty=true ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown argument: %s\n' "$arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$assume_yes" == true && "$mode" != "release" && "$mode" != "publish" ]]; then
  printf '%s\n' "--yes is only valid with --release or --publish." >&2
  exit 2
fi
if [[ "$allow_dirty" == true && "$mode" != "check-only" ]]; then
  printf '%s\n' "--allow-dirty is only valid with --check-only." >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

core_name="another-hatchet-core"
macros_name="another-hatchet-macros"
sdk_name="another-hatchet-sdk"
publish_order=("$core_name" "$macros_name" "$sdk_name")

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

workspace_version() {
  sed -nE 's/^version[[:space:]]*=[[:space:]]*"([^"]+)"/\1/p' Cargo.toml | head -n 1
}

workspace_rust_version() {
  sed -nE 's/^rust-version[[:space:]]*=[[:space:]]*"([^"]+)"/\1/p' Cargo.toml | head -n 1
}

cargo_dirty_flag=()
if [[ "$allow_dirty" == true || "$mode" == "prepare" || "$mode" == "release" ]]; then
  cargo_dirty_flag=(--allow-dirty)
fi

ensure_clean_worktree() {
  if [[ "$allow_dirty" == false && -n "$(git status --porcelain)" ]]; then
    printf '%s\n' \
      "Git worktree is dirty." \
      "Commit or stash changes so the published crates and Git tag identify one source tree." >&2
    exit 1
  fi
}

verify_release_metadata() {
  local version="$1"
  grep -Fq "$sdk_name = \"$version\"" README.md || {
    printf 'README.md does not contain %s = "%s".\n' "$sdk_name" "$version" >&2
    exit 1
  }
  grep -Fq "## [$version]" CHANGELOG.md || {
    printf 'CHANGELOG.md does not contain a %s release section.\n' "$version" >&2
    exit 1
  }
  [[ -s LICENSE ]] || {
    printf '%s\n' "Root LICENSE is missing or empty." >&2
    exit 1
  }
}

package_list() {
  local package="$1"
  cargo package -p "$package" --list "${cargo_dirty_flag[@]}"
}

verify_package_contents() {
  local package="$1"
  local files
  files="$(package_list "$package")"
  grep -Eq '^LICENSE($|[-.])' <<<"$files" || {
    printf '%s package does not include its license text.\n' "$package" >&2
    exit 1
  }
  grep -Fxq 'README.md' <<<"$files" || {
    printf '%s package does not include README.md.\n' "$package" >&2
    exit 1
  }
  if grep -Eq '(^|/)(audits|target|\.github|\.understand-anything)/' <<<"$files"; then
    printf '%s package contains internal repository artifacts.\n' "$package" >&2
    exit 1
  fi

  if [[ "$package" == "$sdk_name" ]]; then
    local proto_count
    proto_count="$(grep -Ec '^proto/.+\.proto$' <<<"$files")"
    if [[ "$proto_count" -ne 7 ]]; then
      printf 'SDK package contains %s protobuf files; expected 7.\n' "$proto_count" >&2
      exit 1
    fi
  fi
}

run_quality_gates() {
  local rust_version
  rust_version="$(workspace_rust_version)"
  [[ -n "$rust_version" ]] || {
    printf '%s\n' "Could not read workspace.package.rust-version from Cargo.toml." >&2
    exit 1
  }

  printf '%s\n' "==> Formatting"
  cargo fmt --all -- --check
  printf '%s\n' "==> Clippy"
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  printf '%s\n' "==> Workspace tests"
  cargo test --workspace --all-targets --all-features
  printf '%s\n' "==> No-default-features tests"
  cargo test -p "$sdk_name" --no-default-features --all-targets
  printf '%s\n' "==> Strict rustdoc"
  RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
  printf '==> Minimum Rust %s workspace check\n' "$rust_version"
  rustup run "$rust_version" cargo check --workspace --all-targets --all-features --locked
  printf '==> Minimum Rust %s standalone Railway check\n' "$rust_version"
  rustup run "$rust_version" cargo check --locked \
    --manifest-path examples/railway-multi-node/Cargo.toml --all-targets
}

create_package_archives() {
  local package
  for package in "${publish_order[@]}"; do
    printf '==> Packaging %s\n' "$package"
    if [[ "$package" == "$sdk_name" ]]; then
      # Cargo normally insists that versioned path dependencies already exist in
      # the registry, which is impossible before the first core/macros release.
      # Local resolution patches let us create the exact SDK archive without
      # changing the normalized manifest that will be uploaded to crates.io.
      cargo package \
        -p "$package" \
        --locked \
        --no-verify \
        "${cargo_dirty_flag[@]}" \
        --config "patch.crates-io.${core_name}.path=\"$repo_root/crates/hatchet-core\"" \
        --config "patch.crates-io.${macros_name}.path=\"$repo_root/crates/hatchet-macros\""
    else
      cargo package -p "$package" --locked "${cargo_dirty_flag[@]}"
    fi
  done
}

verify_isolated_sdk_archive() {
  local version="$1"
  local temp_dir
  temp_dir="$(mktemp -d)"
  trap 'rm -rf "$temp_dir"' RETURN

  local package
  for package in "${publish_order[@]}"; do
    tar -xzf "target/package/${package}-${version}.crate" -C "$temp_dir"
  done

  local sdk_dir="$temp_dir/${sdk_name}-${version}"
  cat >>"$sdk_dir/Cargo.toml" <<PATCHES

[patch.crates-io]
another-hatchet-core = { path = "../${core_name}-${version}" }
another-hatchet-macros = { path = "../${macros_name}-${version}" }
PATCHES

  printf '%s\n' "==> Building exact SDK archive in an isolated consumer directory"
  CARGO_TARGET_DIR="$temp_dir/target" cargo test \
    --manifest-path "$sdk_dir/Cargo.toml" \
    --all-targets \
    --all-features
  CARGO_TARGET_DIR="$temp_dir/target" RUSTDOCFLAGS="-D warnings" cargo doc \
    --manifest-path "$sdk_dir/Cargo.toml" \
    --all-features \
    --no-deps

  local consumer_dir="$temp_dir/consumer"
  mkdir -p "$consumer_dir/src"
  cat >"$consumer_dir/Cargo.toml" <<CONSUMER_MANIFEST
[package]
name = "another-hatchet-sdk-package-consumer"
version = "0.0.0"
edition = "2024"
rust-version = "1.85"
publish = false

[dependencies]
another-hatchet-sdk = { path = "../${sdk_name}-${version}" }

[patch.crates-io]
another-hatchet-core = { path = "../${core_name}-${version}" }
another-hatchet-macros = { path = "../${macros_name}-${version}" }
CONSUMER_MANIFEST
  cat >"$consumer_dir/src/lib.rs" <<'CONSUMER_SOURCE'
use another_hatchet_sdk::RunOpts;

pub fn default_run_options() -> RunOpts {
    RunOpts::default()
}
CONSUMER_SOURCE

  printf '%s\n' "==> Building a minimal downstream consumer of the exact SDK archive"
  CARGO_TARGET_DIR="$temp_dir/target" cargo check \
    --manifest-path "$consumer_dir/Cargo.toml"

  rm -rf "$temp_dir"
  trap - RETURN
}

registry_has_version() {
  local package="$1"
  local version="$2"
  # Force registry resolution so a same-named local workspace package cannot
  # produce a false positive during a first release.
  cargo info "${package}@${version}" --registry crates-io >/dev/null 2>&1
}

publish_dry_run() {
  local package="$1"
  cargo publish --dry-run -p "$package" --locked "${cargo_dirty_flag[@]}"
}

run_publish_checks() {
  local version="$1"
  printf '%s\n' "==> crates.io dry run: $core_name"
  publish_dry_run "$core_name"
  printf '%s\n' "==> crates.io dry run: $macros_name"
  publish_dry_run "$macros_name"

  if registry_has_version "$core_name" "$version" &&
    registry_has_version "$macros_name" "$version"; then
    printf '%s\n' "==> crates.io dry run: $sdk_name"
    publish_dry_run "$sdk_name"
  else
    printf '%s\n' \
      "==> SDK registry dry run deferred: internal $version dependencies are not published yet." \
      "    The exact SDK archive was built above with local patches to the exact packaged dependencies."
  fi
}

run_preflight() {
  local version="$1"
  run_quality_gates
  local package
  for package in "${publish_order[@]}"; do
    verify_package_contents "$package"
  done
  create_package_archives
  verify_isolated_sdk_archive "$version"
  run_publish_checks "$version"
}

sync_readme_version() {
  local version="$1"
  RELEASE_VERSION="$version" perl -0pi -e '
    s/(another-hatchet-sdk\s*=\s*")\d+\.\d+\.\d+(")/$1$ENV{RELEASE_VERSION}$2/g
  ' README.md
  RELEASE_VERSION="$version" perl -0pi -e '
    s/((?:another-hatchet-core|another-hatchet-macros)\s*=\s*\{[^}]*version\s*=\s*")\d+\.\d+\.\d+(")/$1$ENV{RELEASE_VERSION}$2/g
  ' crates/hatchet-sdk/Cargo.toml
}

assert_only_release_files_changed() {
  local path
  while IFS= read -r path; do
    case "$path" in
      Cargo.toml|Cargo.lock|CHANGELOG.md|README.md|crates/hatchet-core/Cargo.toml|crates/hatchet-macros/Cargo.toml|crates/hatchet-sdk/Cargo.toml)
        ;;
      "") ;;
      *)
        printf 'Release preparation changed an unexpected file: %s\n' "$path" >&2
        exit 1
        ;;
    esac
  done < <({
    git diff --name-only
    git diff --cached --name-only
    git ls-files --others --exclude-standard
  } | sort -u)
}

confirm_publish() {
  local version="$1"
  if [[ "$assume_yes" == true ]]; then
    return
  fi
  printf '\nThis permanently publishes version %s of:\n' "$version"
  printf '  - %s\n' "${publish_order[@]}"
  printf 'Type "publish %s" to continue: ' "$version"
  local answer
  read -r answer
  if [[ "$answer" != "publish $version" ]]; then
    printf '%s\n' "Publish cancelled."
    exit 1
  fi
}

wait_for_registry() {
  local package="$1"
  local version="$2"
  local attempt
  for attempt in $(seq 1 30); do
    if registry_has_version "$package" "$version"; then
      return
    fi
    printf 'Waiting for crates.io index: %s %s (%s/30)\n' \
      "$package" "$version" "$attempt"
    sleep 10
  done
  printf 'Timed out waiting for crates.io to index %s %s.\n' "$package" "$version" >&2
  exit 1
}

publish_packages() {
  local version="$1"
  local package
  for package in "${publish_order[@]}"; do
    if registry_has_version "$package" "$version"; then
      printf '==> %s %s is already published; skipping upload.\n' "$package" "$version"
    else
      if [[ "$package" == "$sdk_name" ]]; then
        printf '%s\n' "==> crates.io dry run: $sdk_name"
        publish_dry_run "$sdk_name"
      fi
      printf '==> Publishing %s %s\n' "$package" "$version"
      cargo publish -p "$package" --locked
    fi
    wait_for_registry "$package" "$version"
  done
}

commit_release() {
  local version="$1"
  local branch
  branch="$(git branch --show-current)"
  [[ -n "$branch" ]] || {
    printf '%s\n' "A release must be created from a named Git branch." >&2
    exit 1
  }

  assert_only_release_files_changed
  git add -- \
    Cargo.toml \
    Cargo.lock \
    CHANGELOG.md \
    README.md \
    crates/hatchet-core/Cargo.toml \
    crates/hatchet-macros/Cargo.toml \
    crates/hatchet-sdk/Cargo.toml
  git diff --cached --quiet && {
    printf '%s\n' "Release preparation produced no changes to commit." >&2
    exit 1
  }
  git commit -m "chore: prepare $sdk_name $version release"
  git push origin "HEAD:refs/heads/$branch"
}

ensure_tag_available() {
  local tag="$1"
  if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    printf 'Local Git tag already exists: %s\n' "$tag" >&2
    exit 1
  fi
  if git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null 2>&1; then
    printf 'Remote Git tag already exists: %s\n' "$tag" >&2
    exit 1
  fi
}

require_command cargo
require_command git
require_command grep
require_command perl
require_command rustup
require_command sed
require_command tar

if [[ "$mode" == "prepare" || "$mode" == "release" ]]; then
  require_command release-plz
fi

ensure_clean_worktree

version="$(workspace_version)"
[[ -n "$version" ]] || {
  printf '%s\n' "Could not read workspace.package.version from Cargo.toml." >&2
  exit 1
}

if [[ "$mode" == "prepare" || "$mode" == "release" ]]; then
  printf '%s\n' "==> Preparing versions and changelog with release-plz"
  release-plz update
  version="$(workspace_version)"
  sync_readme_version "$version"
fi

verify_release_metadata "$version"
printf '==> Release candidate: %s %s\n' "$sdk_name" "$version"
run_preflight "$version"

case "$mode" in
  check-only)
    printf '%s\n' "==> Preflight complete; nothing was published."
    ;;
  prepare)
    printf '%s\n' "==> Release metadata prepared and validated. Review and commit the changes."
    ;;
  publish)
    confirm_publish "$version"
    publish_packages "$version"
    printf '%s\n' "==> Published all crates. Create and push tag v$version after verification."
    ;;
  release)
    tag="v$version"
    ensure_tag_available "$tag"
    confirm_publish "$version"
    commit_release "$version"
    publish_packages "$version"
    git tag "$tag"
    git push origin "$tag"
    printf '%s\n' "==> Release $tag published and tagged."
    ;;
esac
