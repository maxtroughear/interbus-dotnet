---
sidebar_position: 1
---

# Outbox pattern

The outbox pattern is a producer-side reliability pattern for avoiding dual-write inconsistency.

## Problem it solves

Without an outbox, a producer often does two separate operations:

1. Commit application/domain state
2. Publish a message

If one succeeds and the other fails, state and messaging drift apart.

## Typical flow

1. Write domain changes and outbox entries in the same transaction boundary
2. Commit once
3. A relay/processor reads pending outbox entries and publishes them
4. Mark or delete delivered entries

## Delivery model

- Usually **at-least-once** delivery
- Consumers must remain idempotent
- Ordering is typically scoped (for example per key/group), not global

## Trade-offs

- Stronger reliability between state changes and message publication
- Extra storage and background processing complexity
- Potential additional delivery latency depending on polling/relay behavior

## Related pattern

- [Inbox pattern](./inbox)
