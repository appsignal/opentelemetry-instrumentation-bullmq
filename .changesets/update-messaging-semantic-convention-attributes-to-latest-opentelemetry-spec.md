---
bump: minor
type: change
---

Update messaging semantic convention attributes to latest OpenTelemetry Semantic Conventions for Messaging Spans spec.

The emitted spans now use the following updated attribute names:

- `messaging.destination` renamed to `messaging.destination.name`
- `messaging.operation` replaced by `messaging.operation.type` (enum: `send`, `create`, `process`) and `messaging.operation.name` (BullMQ-specific method name, e.g. `Queue.add`, `Worker.run`)
- `messaging.message_id` renamed to `messaging.message.id`
- `messaging.consumer_id` renamed to `messaging.client.id`
- `messaging.bullmq.job.bulk.count` replaced by the standard `messaging.batch.message_count`

Span names now follow the `{operation.name} {destination}` format, e.g. `Queue.add myQueue` instead of `myQueue publish`.
