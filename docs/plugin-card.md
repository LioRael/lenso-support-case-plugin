# Support Case v1 Plugin card

## Owner and deletion boundary

The PostgreSQL Plugin owns cases, organization-local case sequences, messages,
activity, command replay receipts, and privacy-retention receipts. Removing its
package, Instance, Capability bindings, and owned schema removes Support Case
behavior without deleting Organizations, memberships, Auth identities, or
Access Control policy.

## Provided behavior

`lenso.support-case@1` provides eight back-office request operations:
create/get/list, update/assign/transition, and add/list messages. The same
release provides four requester-facing operations through
`lenso.support-intake@1`, one final resource decision through
`lenso.support-case-authorization@1`, and the existing
`lenso.data-export-source@1` and `lenso.retention-participant@1` roles with real
PostgreSQL implementations.

The separate `lenso.support-case.agent-tools` adapter provides
`lenso.agent.tool-provider@2` and requires exactly one
`lenso.support-case@1` provider. It owns only the Agent catalog and typed
argument/result adaptation. Removing it removes the Agent surface without
removing Support Case facts or changing requester, resource-authorization,
export, or retention behavior.

The `SUP-N` identifier is stable only within one Organization. Mutations accept
the stable UUID, an idempotency key, and (except create) an expected decimal
revision. Public messages and internal notes share one owned table but are
separated by a storage predicate, not presentation-layer filtering.

## Authorization boundary

The Plugin uses separate exact caller allowlists for back-office business,
requester intake, resource authorization, export, and retention. Back-office
operations require exact-operation Auth assertions. Internal or privileged
actions also require active Organization membership and an Access Control
decision. Intake callers must derive `requester_subject` from a credential they
verified; Support Case then constrains every read and append to that exact
requester. Resource callers receive only a final allow/deny plus canonical case
UUID and never direct table access. Dependency Runtime failures propagate;
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
payloads over the configured limit fail atomically. Attachment storage and
email transport are separate Plugins using the two narrow seams above. v1 has
no SLA timers, full-text search, case merge/split, webhook event role, or
Console contribution.
