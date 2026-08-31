# Lenso Support Case Plugin

A removable, PostgreSQL-backed customer-support backend for Lenso Apps.

The Plugin owns support cases, public replies, internal agent notes, activity,
idempotency receipts, and privacy-retention receipts. It does not own
Organizations, membership, authentication, or RBAC policy.

## Capabilities

The Plugin provides:

- `lenso.support-case@1`: `create_case`, `get_case`, `list_cases`,
  `update_case`, `assign_case`, `transition_case`, `add_message`, and
  `list_messages`.
- `lenso.support-intake@1`: requester-scoped channel operations to open a case,
  append a public message, read one related case, and list only its public
  messages. This is the narrow boundary used by Resend and Help Center.
- `lenso.support-case-authorization@1`: one resource-authority operation used
  by Support Attachment to authorize public/internal read or attach access and
  resolve the canonical case UUID without sharing Support Case tables.
- `lenso.data-export-source@1`: a bounded JSON export of the subject's related
  cases, subject-authored messages, and subject-attributed activity.
- `lenso.retention-participant@1`: durable, idempotent anonymize and delete
  participation without deleting organization-owned case records.

It requires one Provider for each of:

- `lenso.secrets@1`
- `lenso.organization-membership@1`
- `lenso.access-control@1`

Every operation first enforces the exact caller list for its boundary.
Back-office Support Case operations carry an exact-operation Auth assertion;
internal and privileged actions then check live Organization membership and
Access Control. Requester intake never trusts a caller-provided actor: each
configured channel owns the mapping from its verified credential to
`requester_subject`, and Support Case constrains reads/appends to that exact
subject. Resource authorization accepts only configured resource callers and
makes the final case/message relationship decision.

## Domain guarantees

- Every case has a UUID and an organization-local, stable `SUP-N` identifier.
- Every mutation uses an idempotency key. Reusing a key with another operation
  or payload fails closed.
- Case revisions are portable decimal strings. Mutations require
  `expected_revision` and lock the case row before compare-and-swap.
- Listing uses stable keyset cursors, never offset pagination.
- Public messages are visible to related requesters; internal notes require
  `support.cases.internal-note` and are filtered in storage.
- State transitions are explicit: open -> in progress; in progress -> waiting
  or resolved; waiting -> in progress or resolved; resolved -> in progress or
  closed; closed -> in progress.
- PostgreSQL is the only runtime state. Activation refuses a missing or stale
  schema; there is no in-memory fallback.

## Permissions

The Access Control scope is `{ kind: "organization", id: organization_id }`.

- `support.cases.create`
- `support.cases.read`
- `support.cases.write`
- `support.cases.assign`
- `support.cases.transition`
- `support.cases.comment`
- `support.cases.internal-note`

Requesters, creators, and current assignees can read a related case and add a
public message without organization-wide read/comment permission. Editing,
assignment, transitions, and internal notes always require their dedicated
permission.

## Schema lifecycle

`SupportCaseOperator::setup` creates the owned schema and migration ledger.
`SupportCaseOperator::upgrade` applies pending migrations. Runtime activation
only calls `OwnedPostgres::prepare`, so starting the App never performs DDL.

## Verification

```bash
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
lenso-contract-codegen workspace check --manifest-path Cargo.toml
./scripts/check-repository-boundary.sh
./scripts/check-public-packages.sh
```

Set `LENSO_SUPPORT_CASE_TEST_DATABASE_URL` to run the restart and concurrent
revision acceptance slice against PostgreSQL. Without it, that optional test
returns without external I/O.

The two public crates and their manual Trusted Publishing workflow are
documented in [`docs/release-process.md`](docs/release-process.md).

## v1 limits

The existing Data Export Source contract returns inline items without paging.
This Plugin therefore caps its single JSON item at 8 MiB (or a lower configured
limit) and returns runtime resource exhaustion rather than truncating data.
Email transport, attachment bytes/associations, SLA automation, search,
merge/split, and a Console surface remain separately removable Plugins. This
backend provides only their narrow intake and resource-authorization seams.
