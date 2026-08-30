# Release process

Publish the public crates in dependency order:

1. `lenso-capability-support-case`
2. `lenso-support-case-postgres-plugin`

Publication is manual-only from reviewed `main` through
`.github/workflows/release-plz.yml`. Pushes may refresh a Release-plz pull
request but cannot publish. A live run additionally requires `live=true`, the
literal confirmation `publish`, and `main`.

## Trusted Publishers

Configure a separate crates.io Trusted Publisher for each public crate:

- owner: `LioRael`
- repository: `lenso-support-case-plugin`
- workflow: `release-plz.yml`
- environment: unset

Only the confirmed live job receives `id-token: write`. There is no registry
token fallback. Trusted Publishing cannot allocate a new crates.io name, so
allocate each `0.1.0` name once with a temporary new-package-only token, revoke
that token immediately, and use OIDC for subsequent publication.

## Required evidence

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
lenso-contract-codegen workspace check --manifest-path Cargo.toml
./scripts/check-repository-boundary.sh
./scripts/check-public-packages.sh
```

Before publication, run the PostgreSQL acceptance test against a dedicated
database by setting `LENSO_SUPPORT_CASE_TEST_DATABASE_URL`. Generated
Capability projections are locked artifacts and must not be edited by hand.
