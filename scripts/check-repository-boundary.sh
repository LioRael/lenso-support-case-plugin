#!/usr/bin/env bash
set -euo pipefail

expected_crates=$'lenso-capability-support-case\nlenso-support-case-postgres-plugin'
actual_crates="$(find crates -mindepth 2 -maxdepth 2 -name Cargo.toml -print0 | xargs -0 sed -n 's/^name = "\([^"]*\)"/\1/p' | sort)"

if [[ "$actual_crates" != "$expected_crates" ]]; then
  echo "unexpected workspace crate boundary" >&2
  diff -u <(printf '%s\n' "$expected_crates") <(printf '%s\n' "$actual_crates") || true
  exit 1
fi

if rg -n 'path\s*=\s*"(\.\./\.\./|/)' --glob 'Cargo.toml' .; then
  echo "cross-repository or absolute path dependencies are not allowed" >&2
  exit 1
fi

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
  'lenso.data-export-source@1' \
  'lenso.retention-participant@1'; do
  if ! rg -q "$capability" README.md docs crates; then
    echo "documented provided Capability is missing: $capability" >&2
    exit 1
  fi
done
