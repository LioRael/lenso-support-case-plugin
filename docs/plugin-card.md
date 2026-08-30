# Support Case v1 Plugin card

## Owner and deletion boundary

The PostgreSQL Plugin owns cases, organization-local case sequences, messages,
activity, command replay receipts, and privacy-retention receipts. Removing its
package, Instance, Capability bindings, and owned schema removes Support Case
behavior without deleting Organizations, memberships, Auth identities, or
Access Control policy.

## Provided behavior

`lenso.support-case@1` provides eight request operations: create/get/list,
update/assign/transition, and add/list messages. The same Plugin release also
provides the existing `lenso.data-export-source@1` and
`lenso.retention-participant@1` roles with real PostgreSQL implementations.

The `SUP-N` identifier is stable only within one Organization. Mutations accept
the stable UUID, an idempotency key, and (except create) an expected decimal
revision. Public messages and internal notes share one owned table but are
separated by a storage predicate, not presentation-layer filtering.

## Authorization boundary

The Plugin requires exact caller allowlists, exact-operation Auth assertions,
active Organization membership, one bound Access Control Provider, and a final
resource-local relationship check. Dependency Runtime failures propagate;
dependency domain rejection never becomes an allow decision.

Organization-wide permissions do not replace membership. A requester exception
can grant related-case read/public-comment access, but never edit, assignment,
transition, or internal-note authority.

## Lifecycle and durability

Setup and upgrade are explicit operator workflows. Activation resolves the
database URL through Secrets and verifies the authored schema. PostgreSQL
transactions serialize organization-local sequence allocation, command replay,
case revision updates, and retention actions. All mutation and retention
receipts survive Plugin restart.

## Privacy behavior

Export returns cases related through requester, creator, assignee, or authored
message status. Only messages and activity attributed to the subject are
included, so another agent's internal note is not disclosed.

Anonymize replaces subject references while preserving shared case records.
Delete additionally redacts content authored/created by the subject and clears
their assignment. Both modes are idempotent by `action_id`; replay with a
different intent is rejected.

## Honest limits

The upstream export-source role is bounded-inline and has no cursor contract;
payloads over the configured limit fail atomically. v1 has no attachments,
email transport, SLA timers, full-text search, case merge/split, webhook event
role, or Console contribution.

