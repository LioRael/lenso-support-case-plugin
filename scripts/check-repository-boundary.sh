#!/usr/bin/env bash
set -euo pipefail

expected_crates=$'lenso-capability-support-case\nlenso-capability-support-case-authorization\nlenso-capability-support-intake\nlenso-support-case-postgres-plugin'
actual_crates="$(find crates -mindepth 2 -maxdepth 2 -name Cargo.toml -print0 | xargs -0 sed -n 's/^name = "\([^"]*\)"/\1/p' | sort)"

if [[ "$actual_crates" != "$expected_crates" ]]; then
  echo "unexpected workspace crate boundary" >&2
  diff -u <(printf '%s\n' "$expected_crates") <(printf '%s\n' "$actual_crates") || true
  exit 1
fi

expected_path_dependencies=$'./crates/lenso-support-case-postgres-plugin/Cargo.toml:path = "../lenso-capability-support-case"\n./crates/lenso-support-case-postgres-plugin/Cargo.toml:path = "../lenso-capability-support-case-authorization"\n./crates/lenso-support-case-postgres-plugin/Cargo.toml:path = "../lenso-capability-support-intake"'
actual_path_dependencies="$(rg --no-heading --with-filename -o 'path\s*=\s*"[^"]+"' --glob 'Cargo.toml' . | sort)"

if [[ "$actual_path_dependencies" != "$expected_path_dependencies" ]]; then
  echo "unexpected path dependency boundary" >&2
  diff -u \
    <(printf '%s\n' "$expected_path_dependencies") \
    <(printf '%s\n' "$actual_path_dependencies") || true
  exit 1
fi

# Immutable remote coordinates must resolve one Kernel/runtime/protocol type
# universe without relying on a sibling checkout.
cargo_bin="${LENSO_CARGO_BIN:-cargo}"
metadata="$($cargo_bin metadata --locked --format-version=1)"
single_source_packages=(
  lenso
  lenso-app-plan
  lenso-kernel
  lenso-native-adapter
  lenso-native-adapter-macros
  lenso-runtime-codec
  lenso-contract-authoring
  lenso-contract-authoring-macros
  lenso-contract-codegen
  lenso-contract-runtime
  lenso-plugin-authoring
)
for package in "${single_source_packages[@]}"; do
  package_count="$(jq --arg package "$package" '[.packages[] | select(.name == $package)] | length' <<<"$metadata")"
  if [[ "$package_count" != "1" ]]; then
    echo "$package resolved $package_count times; exactly one source is required" >&2
    exit 1
  fi
done

for source_family in \
  'lenso,lenso-native-adapter,lenso-native-adapter-macros,lenso-runtime-codec' \
  'lenso-app-plan,lenso-kernel' \
  'lenso-contract-authoring,lenso-contract-authoring-macros,lenso-contract-codegen,lenso-contract-runtime,lenso-plugin-authoring'; do
  source_count="$(
    jq --arg family "$source_family" '
      ($family | split(",")) as $names
      | [.packages[] | select(.name as $name | $names | index($name)) | .source]
      | unique
      | length
    ' <<<"$metadata"
  )"
  if [[ "$source_count" != "1" ]]; then
    echo "runtime/protocol family resolved from $source_count sources: $source_family" >&2
    exit 1
  fi
done

if rg -n 'HashMap|Mutex<.*Vec|in.memory|memory fallback' crates --glob '*.rs'; then
  echo "ambient in-memory durable state is not allowed" >&2
  exit 1
fi

if rg -n 'lenso-platform-|lenso-module-|HostBuilder|HostLinkedModule|ModuleManifest' \
  Cargo.toml crates README.md docs --glob '!**/generated.rs'; then
  echo "legacy Lenso framework dependency or API found" >&2
  exit 1
fi

if rg -n '/Users/[^/[:space:]]+|[A-Za-z]:\\Users\\' README.md docs; then
  echo "public documentation contains a local absolute path" >&2
  exit 1
fi

if rg -n 'CARGO_REGISTRY_TOKEN|CRATES_IO_TOKEN' .github; then
  echo "registry-token publication fallback is not allowed" >&2
  exit 1
fi

for manifest in crates/*/Cargo.toml; do
  if ! rg -qx 'publish = true' "$manifest"; then
    echo "$manifest is not explicitly publishable" >&2
    exit 1
  fi
done

release_workflow=.github/workflows/release-plz.yml
for release_contract in \
  'id-token: write' \
  "inputs.confirm == 'publish'" \
  'github.ref == '\''refs/heads/main'\'''; do
  if ! rg --fixed-strings --quiet "$release_contract" "$release_workflow"; then
    echo "release workflow is missing required contract: $release_contract" >&2
    exit 1
  fi
done

for capability in \
  'lenso.support-case@1' \
  'lenso.support-intake@1' \
  'lenso.support-case-authorization@1' \
  'lenso.data-export-source@1' \
  'lenso.retention-participant@1'; do
  if ! rg -q "$capability" README.md docs crates; then
    echo "documented provided Capability is missing: $capability" >&2
    exit 1
  fi
done
