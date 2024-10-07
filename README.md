# OpenTelemetry Bullmq Instrumentation for Node.js

[![Node.js CI](https://github.com/appsignal/opentelemetry-instrumentation-bullmq/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/appsignal/opentelemetry-instrumentation-bullmq/actions/workflows/ci.yml)
[![npm version](https://badge.fury.io/js/@appsignal%2Fopentelemetry-instrumentation-bullmq.svg)](https://badge.fury.io/js/@appsignal%2Fopentelemetry-instrumentation-bullmq)

This module provides automatic tracing instrumentation for [BullMQ][bullmq-web-url].

Compatible with OpenTelemetry JS API and SDK `1.0+`.

## Installation

```bash
npm install --save @appsignal/opentelemetry-instrumentation-bullmq
```

### Supported Versions

- `[2.x, 3.x, 4.x, 5.x]`

It's likely that the instrumentation would support earlier versions of BullMQ, but I haven't tested it.

## Usage

OpenTelemetry BullMQ Instrumentation allows the user to automatically collect trace data from BullMQ jobs and workers and export them to the backend of choice.

To load the instrumentation, specify it in the instrumentations list to `registerInstrumentations`. There is currently no configuration option.

```javascript
const { NodeTracerProvider } = require("@opentelemetry/sdk-trace-node");
const { registerInstrumentations } = require("@opentelemetry/instrumentation");
const {
  BullMQInstrumentation,
} = require("@appsignal/opentelemetry-instrumentation-bullmq");

const provider = new NodeTracerProvider();
provider.register();

registerInstrumentations({
  instrumentations: [
    new BullMQInstrumentation({
      // configuration options, see below
    }),
  ],
});
```

## Configuration options

| Name                                  | Type      | Default&nbsp;value | Description                                                                                                                                                                                                  |
|---------------------------------------| --------- | ------------------ |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `emitCreateSpansForBulk`              | `boolean` | `true`             | Whether to emit a create span for each individual job enqueued by `Queue.addBulk` or `FlowProducer.addBulk`. The span representing the overall bulk operation is emitted regardless.                         |
| `emitCreateSpansForFlow`              | `boolean` | `true`             | Whether to emit a create span for each individual job enqueued by `FlowProducer.add` or `FlowProducer.addBulk`. The span representing the overall flow operation is emitted regardless.                      |
| `requireParentSpanForPublish`         | `boolean` | `false`            | Whether to omit emitting a publish span (and the create child spans for it, for bulk and flow operations) when there is no parent span, meaning that the span created would be the root span of a new trace. |
| `useProducerContextAsConsumerParent`  | `boolean` | `false`            | Whether to use the producer context as the parent for the consumer span. Consumer and Producer will share the same TraceId in this case.                                                                     |

## Emitted spans

The instrumentation aims to comply with the [OpenTelemetry Semantic Convention for Messaging Spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/). Whenever possible, attributes from the semantic convention are used in these spans.

| Name                  | Span kind                                                                                                                      | `messaging.bullmq.operation.name` attribute&nbsp;<a href="#emitted-spans-note-1"><sup>\[1\]</sup></a> | Description                                                                                                                                                                    |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `{queueName} publish` | `PRODUCER`                                                                                                                     | `Queue.add`                                                                                           | A new job is added to the queue.                                                                                                                                               |
| `{queueName} publish` | `INTERNAL`&nbsp;<a href="#emitted-spans-note-2"><sup>\[2\]</sup></a>                                                           | `Queue.addBulk`                                                                                       | New jobs are added to the queue in bulk.                                                                                                                                       |
| `{queueName} publish` | `INTERNAL`&nbsp;<a href="#emitted-spans-note-3"><sup>\[3\]</sup></a>                                                           | `FlowProducer.add`                                                                                    | A new job flow is added to a queue.                                                                                                                                            |
| `(bulk) publish`      | `INTERNAL`&nbsp;<a href="#emitted-spans-note-2"><sup>\[2\]</sup></a>&nbsp;<a href="#emitted-spans-note-3"><sup>\[3\]</sup></a> | `FlowProducer.addBulk`                                                                                | New job flows are added to queues in bulk.                                                                                                                                     |
| `{queueName} create`  | `PRODUCER`                                                                                                                     | `Job.add`                                                                                             | Each of the individual jobs added to a queue. Only emitted in bulk or flow operations. Child span of a publish span.&nbsp;<a href="#emitted-spans-note-4"><sup>\[4\]</sup></a> |
| `{queueName} process` | `CONSUMER`                                                                                                                     | `Worker.run`                                                                                          | Each job execution by a worker. Linked to the corresponding producer span.&nbsp;<a href="#emitted-spans-note-5"><sup>\[5\]</sup></a>                                           |

- <a name="emitted-spans-note-1">**\[1\]**</a>: Represents the BullMQ function that was called in the application in order to trigger this span to be emitted.
- <a name="emitted-spans-note-2">**\[2\]**</a>: When the `emitCreateSpansForBulk` configuration option is set to `false`, it is a `PRODUCER` span.
- <a name="emitted-spans-note-3">**\[3\]**</a>: When the `emitCreateSpansForFlow` configuration option is set to `false`, it is a `PRODUCER` span.
- <a name="emitted-spans-note-4">**\[4\]**</a>: Will not be emitted for calls to `Queue.addBulk` and `FlowProducer.addBulk` when the `emitCreateSpansForBulk` configuration option is `false`, or for calls to `FlowProducer.add` and `FlowProducer.addBulk` when the `emitCreateSpansForFlow` configuration option is set to `false`.
- <a name="emitted-spans-note-5">**\[5\]**</a>: The producer span may not have been emitted if the `requireParentSpanForPublish` is set to `true`. In this case, no link is established.

## Useful links

- For more information on OpenTelemetry, visit: <https://opentelemetry.io/>
- For more about OpenTelemetry JavaScript: <https://github.com/open-telemetry/opentelemetry-js>
- For help or feedback on this project, open an issue or submit a PR

## License

Apache 2.0 - See [LICENSE][license-url] for more information.

[license-url]: https://opensource.org/licenses/Apache-2.0
[npm-url]: https://www.npmjs.com/package/@appsignal/opentelemetry-instrumentation-bullmq
[bullmq-web-url]: https://docs.bullmq.io/

## Contributing

Contributions are welcome. Feel free to open an issue or submit a PR. I would like to have this package included in opentelemetry-js-contrib at some point. Until then, it lives here.

BullMQ has a hard dependency on Redis, which means that Redis is (for now) a test dependency for the instrumentations. To run the tests, you should have a redis server running on localhost at the default port. If you have docker installed, you can just do `docker-compose up` and be ready to go.
