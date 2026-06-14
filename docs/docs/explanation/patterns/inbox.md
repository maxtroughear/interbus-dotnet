---
sidebar_position: 2
---

# Inbox pattern

The inbox pattern is a consumer-side reliability pattern for handling duplicate or retried deliveries safely.

## Problem it solves

Message systems commonly provide at-least-once delivery, so the same message can be delivered more than once.

Without inbox state, duplicate deliveries can repeat side effects.

## Typical flow

1. Read message identity (typically message ID + consumer identity)
2. Check inbox state for `(consumer, message-id)`
3. If already completed, skip consumer work
4. If not completed, execute consumer logic and persist completion/progress state

## Delivery model

- Enables idempotent processing under retries and redeliveries
- Can support staged state transitions (for example pending consumer work vs pending follow-up publish)
- Works best when message IDs are stable and consistently provided

## Trade-offs

- Additional persistence and state management
- Slightly more complex consumer pipeline
- Requires clear key design for consumer identity and message identity

## Related pattern

- [Outbox pattern](./outbox)
