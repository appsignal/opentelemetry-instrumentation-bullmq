# OpenTelemetry instrumentation for BullMQ

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
