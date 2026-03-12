# OpenTelemetry instrumentation for BullMQ

## 0.8.0

_Published on 2026-03-12._

### Changed

- Update OpenTelemetry dependencies to v2. (minor [b85d788](https://github.com/appsignal/appsignal-instrumentation-bullmq/commit/b85d78815936d1df5f41cc83ae58305ab245173d))
- Replace the `messaging.bullmq.job.parentOpts.waitChildrenKey` span attribute with `messaging.bullmq.job.parentOpts.addToWaitingChildren`. The old attribute reported an internal Redis key string (`bull:<queue>:waiting-children`) that bullmq exposed up to version 5.61.2. From 5.62.0, bullmq replaced it with an `addToWaitingChildren` boolean flag. The new attribute normalises both representations into a single boolean: `true` when a flow parent job is waiting for its children to complete. (minor [af9eadb](https://github.com/appsignal/appsignal-instrumentation-bullmq/commit/af9eadb68ec09bc86606952d3f9c857144b0221c))
- Update messaging semantic convention attributes to latest OpenTelemetry Semantic Conventions for Messaging Spans spec.

  The emitted spans now use the following updated attribute names:

  - `messaging.destination` renamed to `messaging.destination.name`
  - `messaging.operation` replaced by `messaging.operation.type` (enum: `send`, `create`, `process`) and `messaging.operation.name` (BullMQ-specific method name, e.g. `Queue.add`, `Worker.run`)
  - `messaging.message_id` renamed to `messaging.message.id`
  - `messaging.consumer_id` renamed to `messaging.client.id`
  - `messaging.bullmq.job.bulk.count` replaced by the standard `messaging.batch.message_count`

  Span names now follow the `{operation.name} {destination}` format, e.g. `Queue.add myQueue` instead of `myQueue publish`.

  (minor [9d6e51a](https://github.com/appsignal/appsignal-instrumentation-bullmq/commit/9d6e51a3b86d0b8606eb2a59e4ba52bf8e698279))

## 0.7.3

_Published on 2024-10-08._

### Added

- Add a `useProducerSpanAsConsumerParent` configuration option that defaults to `false`. When set to `true`, instead of establishing a span link from the consumer span to the producer span, the consumer span will be in the same trace, as a child span of the producer span. (patch [5d7ee27](https://github.com/appsignal/appsignal-instrumentation-bullmq/commit/5d7ee278d302d6c06286168184cf0070df40f959))

## 0.7.2

_Published on 2024-10-01._

### Fixed

- Fix importing the package as an ESM module. (patch [814d93a](https://github.com/appsignal/appsignal-instrumentation-bullmq/commit/814d93a1bfb795567db9f87b7c6b897f5cfa8a70))

## 0.7.1

_Published on 2024-06-13._

### Fixed

- Mark BullMQ peer dependency as optional.
- Do not reexport `bullmq` types.

## 0.7.0

Initial release as `@appsignal/opentelemetry-instrumentation-bullmq`, a fork of `@jenniferplusplus/opentelemetry-instrumentation-bullmq`.
