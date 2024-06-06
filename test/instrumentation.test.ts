// import rewiremock from 'rewiremock';
// rewiremock.overrideEntryPoint(module);

import * as assert from "assert";
// const Redis = require('ioredis-mock');
// rewiremock('ioredis').with(Redis);
// rewiremock.enable();

import {
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
  trace,
} from "@opentelemetry/api";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { AsyncHooksContextManager } from "@opentelemetry/context-async-hooks";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import {
  InMemorySpanExporter,
  ReadableSpan,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import type * as bullmq from "bullmq";

import { BullMQInstrumentation } from "../src";
import IORedis from "ioredis";
import {
  BullMQInstrumentationConfig,
  defaultConfig,
} from "../src/instrumentation";

import util from "util";

// rewiremock.disable();

let Queue: typeof bullmq.Queue;
let FlowProducer: typeof bullmq.FlowProducer;
let Worker: typeof bullmq.Worker;

function getWait(): [Promise<any>, Function, Function] {
  let resolve: Function;
  let reject: Function;
  const p = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });

  // @ts-ignore
  return [p, resolve, reject];
}

// function printSpans(spans: ReadableSpan[]) {
//   console.log(
//     util.inspect(
//       spans.map((span) => ({
//         name: span.name,
//         parent: spans.find((s) => s.spanContext().spanId === span.parentSpanId)
//           ?.name,
//         trace: span.spanContext().traceId,
//         kind: SpanKind[span.kind],
//         attributes: span.attributes,
//         events: span.events,
//         links: span.links,
//       })),
//       { depth: null },
//     ),
//   );
// }

function assertSpanParent(span: ReadableSpan, parent: ReadableSpan) {
  assert.strictEqual(span.parentSpanId, parent.spanContext().spanId);
}

function assertDifferentTrace(span: ReadableSpan, parent: ReadableSpan) {
  assert.notStrictEqual(span.parentSpanId, parent.spanContext().spanId);
  assert.notStrictEqual(
    span.spanContext().traceId,
    parent.spanContext().traceId,
  );
}

function assertSpanLink(span: ReadableSpan, linked: ReadableSpan) {
  assert.ok(
    span.links.some(
      (link) =>
        link.context.spanId === linked.spanContext().spanId &&
        link.context.traceId === linked.spanContext().traceId,
    ),
  );
}

function assertRootSpan(span: ReadableSpan) {
  assert.strictEqual(span.parentSpanId, undefined);
}

function assertMessagingSystem(span: ReadableSpan) {
  assert.strictEqual(span.attributes["messaging.system"], "bullmq");
}

function assertContains(
  object: Record<any, unknown>,
  pairs: Record<any, unknown>,
) {
  contextualizeError(() => {
    Object.entries(pairs).forEach(([key, value]) => {
      assert.deepStrictEqual(object[key], value);
    });
  }, { input: object, expected: pairs })
}

function assertDoesNotContain(object: Record<any, unknown>, keys: string[]) {
  keys.forEach((key) => {
    contextualizeError(() => {
      assert.strictEqual(object[key], undefined);
    }, { key })
  });
}

function contextualizeError(fn: () => void, context: Record<string, any>) {
  try {
    fn();
  } catch (e: any) {
    Object.entries(context).forEach(([key, value]) => {
      e.message += `\n${key}: ${util.format(value)}`
    });
    throw e;
  }
}

describe("bullmq", () => {
  const instrumentation = new BullMQInstrumentation();
  const connection = { host: "localhost" };
  const provider = new NodeTracerProvider();
  const memoryExporter = new InMemorySpanExporter();
  const spanProcessor = new SimpleSpanProcessor(memoryExporter);
  provider.addSpanProcessor(spanProcessor);
  const contextManager = new AsyncHooksContextManager();

  beforeEach(() => {
    contextManager.enable();
    context.setGlobalContextManager(contextManager);
    instrumentation.setTracerProvider(provider);
    trace.setGlobalTracerProvider(provider);
    instrumentation.setConfig(defaultConfig);
    instrumentation.enable();
    propagation.setGlobalPropagator(new W3CTraceContextPropagator());

    /* eslint-disable @typescript-eslint/no-var-requires */
    Worker = require("bullmq").Worker;
    Queue = require("bullmq").Queue;
    FlowProducer = require("bullmq").FlowProducer;
    /* eslint-enable @typescript-eslint/no-var-requires */

    const client = new IORedis(connection);
    client.flushall();
  });

  afterEach(() => {
    // printSpans(memoryExporter.getFinishedSpans());
    contextManager.disable();
    contextManager.enable();
    memoryExporter.reset();
    instrumentation.disable();
  });

  describe("Queue", () => {
    it("should not generate any spans when disabled", async () => {
      instrumentation.disable();
      const q = new Queue("queueName", { connection });
      await q.add("jobName", { test: "yes" });

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 0);
    });

    describe("when requireParentSpanForProducer is true", async () => {
      beforeEach(() => {
        instrumentation.setConfig({ requireParentSpanForProducer: true });
      });

      it("should not create a queue span for add when there is no parent span", async () => {
        const q = new Queue("queueName", { connection });
        await q.add("jobName", { test: "yes" });

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 0);
      });

      it("should not create a queue span and job spans for addBulk when there is no parent span", async () => {
        const q = new Queue("queueName", { connection });
        await q.addBulk([
          { name: "jobName1", data: { test: "yes" } },
          { name: "jobName2", data: { test: "yes" } },
        ]);

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 0);
      });

      it("should create a queue span for add when there is a parent span", async () => {
        await trace
          .getTracer("default")
          .startActiveSpan("root", async (rootSpan) => {
            const q = new Queue("queueName", { connection });
            await q.add("jobName", { test: "yes" });

            const spans = memoryExporter.getFinishedSpans();
            assert.strictEqual(spans.length, 1);

            rootSpan.end();
          });
      });

      it("should create a queue span and many job spans for addBulk when there is a parent span", async () => {
        await trace
          .getTracer("default")
          .startActiveSpan("root", async (rootSpan) => {
            const q = new Queue("queueName", { connection });
            await q.addBulk([
              { name: "jobName1", data: { test: "yes" } },
              { name: "jobName2", data: { test: "yes" } },
            ]);

            const spans = memoryExporter.getFinishedSpans();
            assert.strictEqual(spans.length, 3);

            rootSpan.end();
          });
      });
    });

    it("should create a queue span and no job span for add", async () => {
      // These configuration options should not affect its behaviour, as this
      // is neither a bulk operation nor a flow operation.
      instrumentation.setConfig({
        emitJobSpansForBulk: false,
        emitJobSpansForFlow: false,
      });

      const q = new Queue("queueName", { connection });
      await q.add("jobName", { test: "yes" });

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 1);
      spans.forEach(assertMessagingSystem);

      const queueAddSpan = spans.find(
        (span) => span.name === "queueName.jobName Queue.add",
      );
      assert.notStrictEqual(queueAddSpan, undefined);
      assert.strictEqual(queueAddSpan?.kind, SpanKind.PRODUCER);
      assertContains(queueAddSpan?.attributes!, {
        "messaging.destination": "queueName",
        "messaging.bullmq.job.name": "jobName",
      });

      // TODO: why is there no message ID?
      assertDoesNotContain(queueAddSpan?.attributes!, [
        "message.id",
        "messaging.bullmq.job.parentOpts.parentKey",
        "messaging.bullmq.job.parentOpts.flowChildrenKey",
      ]);

      assertRootSpan(queueAddSpan!);
    });

    it("should contain a message id when explicitly provided in the job span", async () => {
      const q = new Queue("queueName", { connection });
      await q.add("jobName", { test: "yes" }, { jobId: "foobar" });

      const spans = memoryExporter.getFinishedSpans();
      const queueAddSpan = spans.find(
        (span) => span.name === "queueName.jobName Queue.add",
      );
      assert.notStrictEqual(queueAddSpan, undefined);
      assertContains(queueAddSpan?.attributes!, {
        "messaging.bullmq.job.name": "jobName",
        "message.id": "foobar",
      });
    });

    it("should contain the job delay in the job span", async () => {
      const q = new Queue("queueName", { connection });
      await q.add("jobName", { test: "yes" }, { delay: 1000 });

      const spans = memoryExporter.getFinishedSpans();
      const queueAddSpan = spans.find(
        (span) => span.name === "queueName.jobName Queue.add",
      );
      assert.notStrictEqual(queueAddSpan, undefined);
      assertContains(queueAddSpan?.attributes!, {
        "messaging.bullmq.job.opts.delay": 1000,
      });
    });

    it("should create a queue span and many job spans for addBulk", async () => {
      const q = new Queue("queueName", { connection });
      await q.addBulk([
        { name: "jobName1", data: { test: "yes" } },
        { name: "jobName2", data: { test: "yes" } },
      ]);

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      spans.forEach(assertMessagingSystem);

      const queueAddBulkSpan = spans.find(
        (span) => span.name === "queueName Queue.addBulk",
      );
      assert.notStrictEqual(queueAddBulkSpan, undefined);
      assertContains(queueAddBulkSpan?.attributes!, {
        "messaging.destination": "queueName",
        "messaging.bullmq.job.bulk.names": ["jobName1", "jobName2"],
        "messaging.bullmq.job.bulk.count": 2,
      });
      assertDoesNotContain(queueAddBulkSpan?.attributes!, [
        "messaging.bullmq.job.name",
      ]);

      const jobAddSpan1 = spans.find(
        (span) => span.name === "queueName.jobName1 Job.addJob",
      );
      const jobAddSpan2 = spans.find(
        (span) => span.name === "queueName.jobName2 Job.addJob",
      );
      assert.notStrictEqual(jobAddSpan1, undefined);
      assert.notStrictEqual(jobAddSpan2, undefined);

      assertSpanParent(jobAddSpan1!, queueAddBulkSpan!);
      assertSpanParent(jobAddSpan2!, queueAddBulkSpan!);
      assertRootSpan(queueAddBulkSpan!);
    });
  });

  it("should not create any job spans for addBulk when emitJobSpansForBulk is false", async () => {
    instrumentation.setConfig({ emitJobSpansForBulk: false });

    const q = new Queue("queueName", { connection });
    await q.addBulk([
      { name: "jobName1", data: { test: "yes" } },
      { name: "jobName2", data: { test: "yes" } },
    ]);

    const spans = memoryExporter.getFinishedSpans();
    assert.strictEqual(spans.length, 1);
    spans.forEach(assertMessagingSystem);

    const queueAddBulkSpan = spans.find(
      (span) => span.name === "queueName Queue.addBulk",
    );
    assert.notStrictEqual(queueAddBulkSpan, undefined);
  });

  describe("FlowProducer", () => {
    it("should not generate any spans when disabled", async () => {
      instrumentation.disable();
      const q = new FlowProducer({ connection });
      await q.add({ name: "jobName", queueName: "queueName" });

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 0);
    });

    describe("when requireParentSpanForProducer is true", async () => {
      beforeEach(() => {
        instrumentation.setConfig({ requireParentSpanForProducer: true });
      });

      it("should not create a queue span for add and job spans when there is no parent span", async () => {
        const q = new FlowProducer({ connection });
        await q.add({
          name: "jobName",
          queueName: "queueName",
          children: [
            {
              name: "childJobName",
              queueName: "childQueueName",
            },
          ],
        });

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 0);
      });

      it("should not create a queue span and job spans for addBulk when there is no parent span", async () => {
        const q = new FlowProducer({ connection });
        await q.addBulk([
          { name: "jobName1", queueName: "queueName", data: { test: "yes" } },
          { name: "jobName2", queueName: "queueName", data: { test: "yes" } },
        ]);

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 0);
      });

      it("should create a queue span for add and job spans when there is a parent span", async () => {
        await trace
          .getTracer("default")
          .startActiveSpan("root", async (rootSpan) => {
            const q = new FlowProducer({ connection });
            await q.add({
              name: "jobName",
              queueName: "queueName",
              children: [
                {
                  name: "childJobName",
                  queueName: "childQueueName",
                },
              ],
            });

            const spans = memoryExporter.getFinishedSpans();
            assert.strictEqual(spans.length, 3);

            rootSpan.end();
          });
      });

      it("should create a queue span and many job spans for addBulk when there is a parent span", async () => {
        await trace
          .getTracer("default")
          .startActiveSpan("root", async (rootSpan) => {
            const q = new FlowProducer({ connection });
            await q.addBulk([
              {
                name: "jobName1",
                queueName: "queueName",
                data: { test: "yes" },
              },
              {
                name: "jobName2",
                queueName: "queueName",
                data: { test: "yes" },
              },
            ]);

            const spans = memoryExporter.getFinishedSpans();
            assert.strictEqual(spans.length, 3);

            rootSpan.end();
          });
      });
    });

    it("should create a queue span and a job span for add", async () => {
      const q = new FlowProducer({ connection });
      await q.add({ name: "jobName", queueName: "queueName" });

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      spans.forEach(assertMessagingSystem);

      const flowProducerAddSpan = spans.find(
        (span) => span.name === "queueName.jobName FlowProducer.add",
      );
      assert.notStrictEqual(flowProducerAddSpan, undefined);
      assertContains(flowProducerAddSpan?.attributes!, {
        "messaging.destination": "queueName",
        "messaging.bullmq.job.name": "jobName",
      });

      const jobAddSpan = spans.find(
        (span) => span.name === "queueName.jobName Job.addJob",
      );
      assert.notStrictEqual(jobAddSpan, undefined);
      assertContains(jobAddSpan?.attributes!, {
        "messaging.destination": "queueName",
        "messaging.bullmq.job.name": "jobName",
      });
      // TODO: rename to `messaging.message.id`
      assert.strictEqual(
        typeof jobAddSpan?.attributes!["message.id"],
        "string",
      );
      assertDoesNotContain(jobAddSpan?.attributes!, [
        "messaging.bullmq.job.parentOpts.parentKey",
        "messaging.bullmq.job.parentOpts.flowChildrenKey",
      ]);

      assertSpanParent(jobAddSpan!, flowProducerAddSpan!);
      assertRootSpan(flowProducerAddSpan!);
    });

    it("should create a queue span and many job spans for add with children", async () => {
      // This configuration option should not affect its behaviour, as this is
      // not a bulk operation, but a flow operation.
      instrumentation.setConfig({ emitJobSpansForBulk: false });

      const q = new FlowProducer({ connection });
      await q.add({
        name: "jobName",
        queueName: "queueName",
        children: [
          {
            name: "childJobName",
            queueName: "childQueueName",
          },
        ],
      });

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      spans.forEach(assertMessagingSystem);

      const flowProducerAddSpan = spans.find(
        (span) => span.name === "queueName.jobName FlowProducer.add",
      );
      assert.notStrictEqual(flowProducerAddSpan, undefined);
      assert.strictEqual(flowProducerAddSpan?.kind, SpanKind.INTERNAL);
      assertContains(flowProducerAddSpan?.attributes!, {
        "messaging.destination": "queueName",
        "messaging.bullmq.job.name": "jobName",
      });

      const jobAddSpan = spans.find(
        (span) => span.name === "queueName.jobName Job.addJob",
      );
      assert.notStrictEqual(jobAddSpan, undefined);
      assert.strictEqual(jobAddSpan?.kind, SpanKind.PRODUCER);
      assertContains(jobAddSpan?.attributes!, {
        "messaging.destination": "queueName",
        "messaging.bullmq.job.name": "jobName",
        "messaging.bullmq.job.parentOpts.waitChildrenKey":
          "bull:queueName:waiting-children",
      });
      assert.strictEqual(
        typeof jobAddSpan?.attributes!["message.id"],
        "string",
      );
      assertDoesNotContain(jobAddSpan?.attributes!, [
        "messaging.bullmq.job.parentOpts.parentKey",
      ]);

      const jobId = jobAddSpan?.attributes!["message.id"] as string;

      const childJobAddSpan = spans.find(
        (span) => span.name === "childQueueName.childJobName Job.addJob",
      );
      assert.notStrictEqual(childJobAddSpan, undefined);
      assert.strictEqual(childJobAddSpan?.kind, SpanKind.PRODUCER);
      assertContains(childJobAddSpan?.attributes!, {
        "messaging.destination": "childQueueName",
        "messaging.bullmq.job.name": "childJobName",
        "messaging.bullmq.job.opts.parent.id": `${jobId}`,
        // TODO: should this just be `queueName`, without `bull:`?
        // (this seems like a Redis key name)
        "messaging.bullmq.job.opts.parent.queue": "bull:queueName",
        "messaging.bullmq.job.parentOpts.parentKey": `bull:queueName:${jobId}`,
      });
      assert.strictEqual(
        typeof childJobAddSpan?.attributes!["message.id"],
        "string",
      );
      assert.notStrictEqual(
        childJobAddSpan?.attributes!["message.id"],
        "unknown",
      );
      assert.notStrictEqual(childJobAddSpan?.attributes!["message.id"], jobId);
      assertDoesNotContain(childJobAddSpan?.attributes!, [
        "messaging.bullmq.job.parentOpts.waitChildrenKey",
      ]);

      assertSpanParent(jobAddSpan!, flowProducerAddSpan!);
      assertSpanParent(childJobAddSpan!, flowProducerAddSpan!);
      assertRootSpan(flowProducerAddSpan!);
    });

    it("should not create any job spans for add with children when emitJobSpansForFlow is false", async () => {
      instrumentation.setConfig({ emitJobSpansForFlow: false });

      const q = new FlowProducer({ connection });
      await q.add({
        name: "jobName",
        queueName: "queueName",
        children: [
          {
            name: "childJobName",
            queueName: "childQueueName",
          },
        ],
      });

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 1);
      spans.forEach(assertMessagingSystem);

      const flowProducerAddSpan = spans.find(
        (span) => span.name === "queueName.jobName FlowProducer.add",
      );
      assert.notStrictEqual(flowProducerAddSpan, undefined);
      assert.strictEqual(flowProducerAddSpan?.kind, SpanKind.PRODUCER);
    });

    it("should create a queue span and many job spans for addBulk", async () => {
      const q = new FlowProducer({ connection });
      await q.addBulk([
        { name: "jobName1", queueName: "queueName" },
        { name: "jobName2", queueName: "queueName" },
      ]);

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      spans.forEach(assertMessagingSystem);

      const flowProducerAddBulkSpan = spans.find(
        (span) => span.name === "FlowProducer.addBulk",
      );
      assert.notStrictEqual(flowProducerAddBulkSpan, undefined);
      assert.strictEqual(flowProducerAddBulkSpan?.kind, SpanKind.INTERNAL);

      assertContains(flowProducerAddBulkSpan?.attributes!, {
        "messaging.bullmq.job.bulk.names": ["jobName1", "jobName2"],
        "messaging.bullmq.job.bulk.count": 2,
      });
      assertDoesNotContain(flowProducerAddBulkSpan?.attributes!, [
        "messaging.destination",
        "messaging.bullmq.job.name",
      ]);

      const jobAddSpan1 = spans.find(
        (span) => span.name === "queueName.jobName1 Job.addJob",
      );
      const jobAddSpan2 = spans.find(
        (span) => span.name === "queueName.jobName2 Job.addJob",
      );

      for (const jobAddSpan of [jobAddSpan1, jobAddSpan2]) {
        assert.notStrictEqual(jobAddSpan, undefined);
        assert.strictEqual(jobAddSpan?.kind, SpanKind.PRODUCER);
        assertSpanParent(jobAddSpan!, flowProducerAddBulkSpan!);
      }

      assertRootSpan(flowProducerAddBulkSpan!);
    });

    for (const [condition, config] of [
      [
        "emitJobSpansForBulk is false",
        {
          emitJobSpansForBulk: false,
          emitJobSpansForFlow: true,
        },
      ],
      [
        "emitJobSpansForFlow is false",
        {
          emitJobSpansForBulk: true,
          emitJobSpansForFlow: false,
        },
      ],
      [
        "both emitJobSpansForBulk and emitJobSpansForFlow are false",
        {
          emitJobSpansForBulk: false,
          emitJobSpansForFlow: false,
        },
      ],
    ] as [string, BullMQInstrumentationConfig][]) {
      it(`should not create any job spans for addBulk when ${condition}`, async () => {
        instrumentation.setConfig(config);

        const q = new FlowProducer({ connection });
        await q.addBulk([
          { name: "jobName1", queueName: "queueName" },
          { name: "jobName2", queueName: "queueName" },
        ]);

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 1);
        spans.forEach(assertMessagingSystem);

        const flowProducerAddBulkSpan = spans.find(
          (span) => span.name === "FlowProducer.addBulk",
        );
        assert.notStrictEqual(flowProducerAddBulkSpan, undefined);
        assert.strictEqual(flowProducerAddBulkSpan?.kind, SpanKind.PRODUCER);
      });
    }
  });

  describe("Worker", () => {
    it("should not generate any spans when disabled", async () => {
      const [processor, processorDone] = getWait();

      instrumentation.disable();
      const w = new Worker(
        "disabled",
        async () => {
          processorDone();
          return { completed: new Date().toTimeString() };
        },
        { connection },
      );
      await w.waitUntilReady();

      const q = new Queue("disabled", { connection });
      await q.add("testJob", { test: "yes" });

      await processor;
      await w.close();

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 0);
    });

    it("should create a span for the worker run and job attempt", async () => {
      const [processor, processorDone] = getWait();

      const w = new Worker(
        "queueName",
        async () => {
          processorDone();
          return { completed: new Date().toTimeString() };
        },
        { connection },
      );
      await w.waitUntilReady();

      const q = new Queue("queueName", { connection });
      await q.add("testJob", { test: "yes" });

      await processor;
      await w.close();

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      spans.forEach(assertMessagingSystem);

      const queueAddSpan = spans.find(
        (span) => span.name === "queueName.testJob Queue.add",
      );
      assert.notStrictEqual(queueAddSpan, undefined);

      const workerJobSpan = spans.find((span) =>
        span.name.includes("queueName.testJob Worker.queueName"),
      );
      assert.notStrictEqual(workerJobSpan, undefined);
      assert.strictEqual(workerJobSpan?.kind, SpanKind.CONSUMER);
      assertDifferentTrace(workerJobSpan!, queueAddSpan!);
      assertSpanLink(workerJobSpan!, queueAddSpan!);
      assertContains(workerJobSpan?.attributes!, {
        "messaging.consumer_id": "queueName",
        "messaging.message_id": "1",
        "messaging.operation": "receive",
        "messaging.bullmq.job.name": "testJob",
        "messaging.bullmq.queue.name": "queueName",
        "messaging.bullmq.worker.name": "queueName",
        "messaging.bullmq.worker.concurrency": 1,
        "messaging.bullmq.worker.lockDuration": 30000,
        "messaging.bullmq.worker.lockRenewTime": 15000,
      });

      // Attempts start from 0 in BullMQ 5, and from 1 in BullMQ 4 or earlier
      assert.ok(
        (workerJobSpan?.attributes![
          "messaging.bullmq.job.attempts"
        ] as number) < 2,
      );
      assert.strictEqual(
        typeof workerJobSpan?.attributes!["messaging.bullmq.job.timestamp"],
        "number",
      );
      assert.strictEqual(
        typeof workerJobSpan?.attributes!["messaging.bullmq.job.processedOn"],
        "number",
      );
      assert.ok(
        workerJobSpan?.attributes!["messaging.bullmq.job.processedOn"]! >=
          workerJobSpan?.attributes!["messaging.bullmq.job.timestamp"]!,
      );

      // no error event
      assert.strictEqual(workerJobSpan?.events.length, 0);
    });

    it("should set the right active context for the job attempt", async () => {
      const [processor, processorDone] = getWait();

      const w = new Worker(
        "queueName",
        async () => {
          trace
            .getTracer("test-tracer")
            .startActiveSpan("inside job", (span) => span.end());
          processorDone();
          return { completed: new Date().toTimeString() };
        },
        { connection },
      );
      await w.waitUntilReady();

      const q = new Queue("queueName", { connection });
      await q.add("testJob", { test: "yes" });

      await processor;
      await w.close();

      const spans = memoryExporter.getFinishedSpans();
      const workerJobSpan = spans.find((span) =>
        span.name.includes("queueName.testJob Worker.queueName"),
      );
      assert.notStrictEqual(workerJobSpan, undefined);

      const insideJobSpan = spans.find((span) => span.name === "inside job");
      assert.notStrictEqual(insideJobSpan, undefined);

      assertSpanParent(insideJobSpan!, workerJobSpan!);
    });

    it("should capture events from the processor", async () => {
      const [processor, processorDone] = getWait();

      const q = new Queue("worker", { connection });
      const w = new Worker(
        "worker",
        async (job, token) => {
          await job.extendLock(token as string, 20);
          processorDone();
          return { completed: new Date().toTimeString() };
        },
        { connection },
      );
      await w.waitUntilReady();

      await q.add("testJob", { started: new Date().toTimeString() });

      await processor;
      await w.close();

      const span = memoryExporter
        .getFinishedSpans()
        .find((span) => span.name.includes("Worker.worker"));
      const evt = span?.events.find((event) =>
        event.name.includes("extendLock"),
      );

      assert.notStrictEqual(evt, undefined);
    });

    it("should capture errors from the processor", async () => {
      const [processor, processorDone] = getWait();

      const q = new Queue("worker", { connection });
      const w = new Worker(
        "worker",
        async () => {
          processorDone();
          throw new Error("forced error");
        },
        { connection },
      );
      await w.waitUntilReady();

      await q.add("testJob", { started: new Date().toTimeString() });

      await processor;
      await w.close();

      const span = memoryExporter
        .getFinishedSpans()
        .find((span) => span.name.includes("Worker.worker"));
      const evt = span?.events.find((event) =>
        event.name.includes("exception"),
      );

      assert.notStrictEqual(evt, undefined);
      assert.strictEqual(span?.status.code, SpanStatusCode.ERROR);
      assert.strictEqual(span?.status.message, "forced error");
    });

    it("should create spans for each job attempt", async () => {
      const [processor, processorDone] = getWait();
      let attemptedOnce = false;

      const q = new Queue("worker", { connection });
      const w = new Worker(
        "worker",
        async () => {
          if (!attemptedOnce) {
            attemptedOnce = true;
            throw new Error("forced error");
          }
          processorDone();
          return { completed: new Date().toTimeString() };
        },
        { connection },
      );
      await w.waitUntilReady();

      await q.add(
        "testJob",
        { started: new Date().toTimeString() },
        { attempts: 3 },
      );

      await processor;
      await w.close();

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      spans.forEach(assertMessagingSystem);

      const jobSpans = spans.filter((span) =>
        span.name.includes("Worker.worker"),
      );
      assert.strictEqual(jobSpans.length, 2);
      jobSpans.forEach((span) => {
        assert.strictEqual(
          typeof span.attributes["messaging.bullmq.job.attempts"],
          "number",
        );
      });

      jobSpans.sort((a, b) => {
        const aAttempts = a.attributes![
          "messaging.bullmq.job.attempts"
        ] as number;
        const bAttempts = b.attributes![
          "messaging.bullmq.job.attempts"
        ] as number;
        return aAttempts - bAttempts;
      });

      const firstJobSpan = jobSpans[0];
      assert.notStrictEqual(firstJobSpan, undefined);
      assertDoesNotContain(firstJobSpan?.attributes!, [
        "messaging.bullmq.job.failedReason",
      ]);
      assert.strictEqual(firstJobSpan?.events.length, 1);

      const secondJobSpan = jobSpans[1];
      assert.notStrictEqual(secondJobSpan, undefined);
      assertContains(secondJobSpan?.attributes!, {
        "messaging.bullmq.job.failedReason": "forced error",
      });
      assert.strictEqual(secondJobSpan?.events.length, 0);

      assert.strictEqual(
        (secondJobSpan.attributes!["messaging.bullmq.job.attempts"] as number) -
          (firstJobSpan.attributes!["messaging.bullmq.job.attempts"] as number),
        1,
      );
    });
  });
});
