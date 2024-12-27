import {
  InstrumentationBase,
  InstrumentationConfig,
  InstrumentationNodeModuleDefinition,
} from "@opentelemetry/instrumentation";
import type { Attributes, Link, Span } from "@opentelemetry/api";
import {
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
  trace,
} from "@opentelemetry/api";
import type * as bullmq from "bullmq";
import type {
  FlowJob,
  FlowOpts,
  FlowProducer,
  Job,
  JobNode,
  JobsOptions,
  ParentOpts,
  Queue,
  Worker,
} from "bullmq";
import { flatten } from "flat";

import { VERSION } from "./version";
import { SemanticAttributes, BullMQAttributes } from "./attributes";

declare type Fn = (...args: any[]) => any;
const BULK_CONTEXT = Symbol("BULLMQ_BULK_CONTEXT");
const FLOW_CONTEXT = Symbol("BULLMQ_FLOW_CONTEXT");

export interface BullMQInstrumentationConfig extends InstrumentationConfig {
  /**
   * Emit spans for each individual job enqueueing in calls to `Queue.addBulk`
   * or `FlowProducer.addBulk`. Defaults to `true`. Setting it to `false` disables
   * individual job spans for bulk operations.
   */
  emitCreateSpansForBulk?: boolean;

  /**
   * Emit spans for each individual job enqueueing in calls to `FlowProducer.add`
   * or `FlowProducer.addBulk`. Defaults to `true`. Setting it to `false` disables
   * individual job spans for bulk operations.
   */
  emitCreateSpansForFlow?: boolean;

  /**
   * Require a parent span in order to create a producer span, that is, a span
   * for the enqueueing of one or more jobs. Defaults to `false`.
   */
  requireParentSpanForPublish?: boolean;

  /**
   * Whether to use the producer kind (create or publish) span as the parent
   * for the consumer kind (process) span. When set to `true`, the consumer and
   * producer spans will be part of the same trace. When set to `false`, the
   * consumer span will be in a separate trace from the producer span, and it
   * will contain a link to the producer span. Defaults to `true`.
   */
  useProducerSpanAsConsumerParent?: boolean;
}

export const defaultConfig: Required<BullMQInstrumentationConfig> = {
  emitCreateSpansForBulk: true,
  emitCreateSpansForFlow: true,
  requireParentSpanForPublish: false,
  useProducerSpanAsConsumerParent: false,
  // unused by `configFor` but required for the type
  enabled: true,
};

export class BullMQInstrumentation extends InstrumentationBase {
  protected override _config!: BullMQInstrumentationConfig;

  constructor(config: BullMQInstrumentationConfig = {}) {
    super("@appsignal/opentelemetry-instrumentation-bullmq", VERSION, config);
  }

  override setConfig(config: BullMQInstrumentationConfig) {
    super.setConfig(config);
  }

  /**
   * Init method will be called when the plugin is constructed.
   * It returns an `InstrumentationNodeModuleDefinition` which describes
   *   the node module to be instrumented and patched.
   * It may also return a list of `InstrumentationNodeModuleDefinition`s if
   *   the plugin should patch multiple modules or versions.
   */
  protected init() {
    return new InstrumentationNodeModuleDefinition(
      "bullmq",
      ["1.*", "2.*", "3.*", "4.*", "5.*"],
      this._onPatchMain(),
      this._onUnPatchMain(),
    );
  }

  private _onPatchMain() {
    return (moduleExports: typeof bullmq) => {
      this._diag.debug(`patching ${this.instrumentationName}@${VERSION}`);

      // As Spans
      this._wrap(moduleExports.Queue.prototype, "add", this._patchQueueAdd());
      this._wrap(
        moduleExports.Queue.prototype,
        "addBulk",
        this._patchQueueAddBulk(),
      );
      this._wrap(
        moduleExports.FlowProducer.prototype,
        "add",
        this._patchFlowProducerAdd(),
      );
      this._wrap(
        moduleExports.FlowProducer.prototype,
        "addBulk",
        this._patchFlowProducerAddBulk(),
      );
      this._wrap(moduleExports.Job.prototype, "addJob", this._patchAddJob());

      this._wrap(
        moduleExports.Worker.prototype,
        "callProcessJob" as any,
        this._patchCallProcessJob(),
      );

      // As Events
      this._wrap(
        moduleExports.Job.prototype,
        "extendLock",
        this._patchExtendLock(),
      );
      this._wrap(moduleExports.Job.prototype, "remove", this._patchRemove());
      this._wrap(moduleExports.Job.prototype, "retry", this._patchRetry());

      return moduleExports;
    };
  }

  private _onUnPatchMain() {
    return (moduleExports: typeof bullmq) => {
      this._diag.debug(`un-patching ${this.instrumentationName}@${VERSION}`);

      this._unwrap(moduleExports.Queue.prototype, "add");
      this._unwrap(moduleExports.Queue.prototype, "addBulk");
      this._unwrap(moduleExports.FlowProducer.prototype, "add");
      this._unwrap(moduleExports.FlowProducer.prototype, "addBulk");
      this._unwrap(moduleExports.Job.prototype, "addJob");

      this._unwrap(moduleExports.Worker.prototype, "callProcessJob" as any);

      this._unwrap(moduleExports.Job.prototype, "extendLock");
      this._unwrap(moduleExports.Job.prototype, "remove");
      this._unwrap(moduleExports.Job.prototype, "retry");
    };
  }

  private _patchAddJob(): (original: Function) => (...args: any) => any {
    const instrumentation = this;
    const tracer = instrumentation.tracer;
    const operationName = "Job.addJob";
    const operationType = "create";

    return function addJob(original) {
      return async function patch(
        this: Job,
        client: never,
        parentOpts?: ParentOpts,
      ): Promise<string> {
        const parentContext = context.active();
        let parentSpan = trace.getSpan(parentContext);

        if (parentSpan === undefined) {
          // This can happen when `requireParentSpanForPublish` is true.
          return await original.apply(this, [client, parentOpts]);
        }

        const isBulk: boolean = !!parentContext.getValue(BULK_CONTEXT);
        const isFlow: boolean = !!parentContext.getValue(FLOW_CONTEXT);

        let shouldSetAttributes = true;

        if (!instrumentation.shouldCreateSpan({ isBulk, isFlow })) {
          // If the configuration says that no individual job spans
          // should be created for this bulk/flow span, do not set
          // attributes in the parent span either.
          // This differs from the behaviour when the span is neither
          // bulk nor flow, in which case we do write attributes into
          // the parent span.
          shouldSetAttributes = false;
        }

        const shouldCreateSpan =
          (isBulk || isFlow) &&
          instrumentation.shouldCreateSpan({ isBulk, isFlow });

        let childSpan: Span | undefined;

        if (shouldCreateSpan) {
          const spanName = `${this.queueName} ${operationType}`;
          childSpan = tracer.startSpan(spanName, {
            kind: SpanKind.PRODUCER,
            attributes: {
              [SemanticAttributes.MESSAGING_OPERATION]: operationType,
              [BullMQAttributes.MESSAGING_OPERATION_NAME]: operationName,
            },
          });
        }

        let span = childSpan ?? parentSpan;

        if (shouldSetAttributes) {
          span.setAttributes(
            BullMQInstrumentation.dropInvalidAttributes({
              [SemanticAttributes.MESSAGING_SYSTEM]:
                BullMQAttributes.MESSAGING_SYSTEM,
              [SemanticAttributes.MESSAGING_DESTINATION]: this.queueName,
              [BullMQAttributes.JOB_NAME]: this.name,
              [BullMQAttributes.JOB_PARENT_KEY]: parentOpts?.parentKey,
              [BullMQAttributes.JOB_WAIT_CHILDREN_KEY]:
                parentOpts?.waitChildrenKey,
              ...BullMQInstrumentation.attrMap(
                BullMQAttributes.JOB_OPTS,
                this.opts,
              ),
            }),
          );
        }

        const messageContext = trace.setSpan(parentContext, span);

        propagation.inject(messageContext, this.opts);
        return await context.with(messageContext, async () => {
          try {
            return await original.apply(this, [client, parentOpts]);
          } catch (e) {
            throw BullMQInstrumentation.setError(span, e as Error);
          } finally {
            if (shouldSetAttributes) {
              span.setAttributes(
                BullMQInstrumentation.dropInvalidAttributes({
                  [SemanticAttributes.MESSAGING_MESSAGE_ID]: this.id,
                  [BullMQAttributes.JOB_TIMESTAMP]: this.timestamp,
                }),
              );
            }
            if (shouldCreateSpan) {
              span.end();
            }
          }
        });
      };
    };
  }

  private _patchQueueAdd(): (original: Function) => (...args: any) => any {
    const instrumentation = this;
    const tracer = instrumentation.tracer;
    const operationName = "Queue.add";
    const operationType = "publish";

    return function add(original) {
      return async function patch(this: Queue, ...args: any): Promise<Job> {
        if (
          instrumentation.configFor("requireParentSpanForPublish") &&
          trace.getSpan(context.active()) === undefined
        ) {
          return await original.apply(this, args);
        }

        const [name] = [...args];

        const spanName = `${this.name} ${operationType}`;
        const span = tracer.startSpan(spanName, {
          kind: SpanKind.PRODUCER,
          attributes: {
            [SemanticAttributes.MESSAGING_OPERATION]: operationType,
            [BullMQAttributes.MESSAGING_OPERATION_NAME]: operationName,
          },
        });

        return BullMQInstrumentation.withContext(this, original, span, args);
      };
    };
  }

  private _patchQueueAddBulk(): (original: Function) => (...args: any) => any {
    const instrumentation = this;
    const tracer = instrumentation.tracer;
    const operationName = "Queue.addBulk";
    const operationType = "publish";

    return function addBulk(original) {
      return async function patch(
        this: bullmq.Queue,
        ...args: [bullmq.Job[], ...any]
      ): Promise<bullmq.Job[]> {
        if (
          instrumentation.configFor("requireParentSpanForPublish") &&
          trace.getSpan(context.active()) === undefined
        ) {
          return await original.apply(this, args);
        }

        const names = args[0].map((job) => job.name);

        const spanName = `${this.name} ${operationType}`;
        const spanKind = instrumentation.shouldCreateSpan({
          isBulk: true,
          isFlow: false,
        })
          ? SpanKind.INTERNAL
          : SpanKind.PRODUCER;

        const span = tracer.startSpan(spanName, {
          attributes: {
            [SemanticAttributes.MESSAGING_SYSTEM]:
              BullMQAttributes.MESSAGING_SYSTEM,
            [SemanticAttributes.MESSAGING_DESTINATION]: this.name,
            [SemanticAttributes.MESSAGING_OPERATION]: operationType,
            [BullMQAttributes.MESSAGING_OPERATION_NAME]: operationName,
            [BullMQAttributes.JOB_BULK_NAMES]: names,
            [BullMQAttributes.JOB_BULK_COUNT]: names.length,
          },
          kind: spanKind,
        });

        return BullMQInstrumentation.withContext(this, original, span, args, {
          [BULK_CONTEXT]: true,
        });
      };
    };
  }

  private _patchFlowProducerAdd(): (
    original: Function,
  ) => (...args: any) => any {
    const instrumentation = this;
    const tracer = instrumentation.tracer;
    const operationName = "FlowProducer.add";
    const operationType = "publish";

    return function add(original) {
      return async function patch(
        this: FlowProducer,
        flow: FlowJob,
        opts?: FlowOpts,
      ): Promise<JobNode> {
        if (
          instrumentation.configFor("requireParentSpanForPublish") &&
          trace.getSpan(context.active()) === undefined
        ) {
          return await original.apply(this, [flow, opts]);
        }

        const spanName = `${flow.queueName} ${operationType}`;
        const spanKind = instrumentation.shouldCreateSpan({
          isBulk: false,
          isFlow: true,
        })
          ? SpanKind.INTERNAL
          : SpanKind.PRODUCER;

        const span = tracer.startSpan(spanName, {
          attributes: {
            [SemanticAttributes.MESSAGING_SYSTEM]:
              BullMQAttributes.MESSAGING_SYSTEM,
            [SemanticAttributes.MESSAGING_DESTINATION]: flow.queueName,
            [SemanticAttributes.MESSAGING_OPERATION]: operationType,
            [BullMQAttributes.MESSAGING_OPERATION_NAME]: operationName,
            [BullMQAttributes.JOB_NAME]: flow.name,
          },
          kind: spanKind,
        });

        return BullMQInstrumentation.withContext(
          this,
          original,
          span,
          [flow, opts],
          {
            [FLOW_CONTEXT]: true,
          },
        );
      };
    };
  }

  private _patchFlowProducerAddBulk(): (
    original: Function,
  ) => (...args: any) => any {
    const instrumentation = this;
    const tracer = instrumentation.tracer;
    const operationName = "FlowProducer.addBulk";
    const operationType = "publish";

    return function addBulk(original) {
      return async function patch(
        this: FlowProducer,
        ...args: [FlowJob[], ...any]
      ): Promise<JobNode> {
        if (
          instrumentation.configFor("requireParentSpanForPublish") &&
          trace.getSpan(context.active()) === undefined
        ) {
          return await original.apply(this, args);
        }

        const spanName = `(bulk) ${operationType}`;
        const spanKind = instrumentation.shouldCreateSpan({
          isBulk: true,
          isFlow: true,
        })
          ? SpanKind.INTERNAL
          : SpanKind.PRODUCER;

        const names = args[0].map((job) => job.name);
        const span = tracer.startSpan(spanName, {
          attributes: {
            [SemanticAttributes.MESSAGING_SYSTEM]:
              BullMQAttributes.MESSAGING_SYSTEM,
            [SemanticAttributes.MESSAGING_OPERATION]: operationType,
            [BullMQAttributes.MESSAGING_OPERATION_NAME]: operationName,
            [BullMQAttributes.JOB_BULK_NAMES]: names,
            [BullMQAttributes.JOB_BULK_COUNT]: names.length,
          },
          kind: spanKind,
        });

        return BullMQInstrumentation.withContext(this, original, span, args, {
          [FLOW_CONTEXT]: true,
          [BULK_CONTEXT]: true,
        });
      };
    };
  }

  private _patchCallProcessJob(): (
    original: Function,
  ) => (...args: any) => any {
    const instrumentation = this;
    const tracer = instrumentation.tracer;
    const operationType = "process";
    const operationName = "Worker.run";

    return function patch(original) {
      return async function callProcessJob(
        this: Worker,
        job: any,
        ...rest: any[]
      ) {
        const producerParent = instrumentation.configFor(
          "useProducerSpanAsConsumerParent",
        );

        const workerName = this.name ?? "anonymous";
        const currentContext = context.active();
        const producerContext = propagation.extract(currentContext, job.opts);

        const parentContext = producerParent ? producerContext : currentContext;
        const links = BullMQInstrumentation.dropInvalidLinks([
          {
            context: producerParent
              ? undefined
              : trace.getSpanContext(producerContext),
          },
        ]);

        const spanName = `${job.queueName} ${operationType}`;
        const span = tracer.startSpan(
          spanName,
          {
            attributes: BullMQInstrumentation.dropInvalidAttributes({
              [SemanticAttributes.MESSAGING_SYSTEM]:
                BullMQAttributes.MESSAGING_SYSTEM,
              [SemanticAttributes.MESSAGING_CONSUMER_ID]: workerName,
              [SemanticAttributes.MESSAGING_MESSAGE_ID]: job.id,
              [SemanticAttributes.MESSAGING_OPERATION]: operationType,
              [BullMQAttributes.MESSAGING_OPERATION_NAME]: operationName,
              [BullMQAttributes.JOB_NAME]: job.name,
              [BullMQAttributes.JOB_ATTEMPTS]: job.attemptsMade,
              [BullMQAttributes.JOB_TIMESTAMP]: job.timestamp,
              [BullMQAttributes.JOB_DELAY]: job.delay,
              [BullMQAttributes.JOB_REPEAT_KEY]: job.repeatJobKey,
              ...BullMQInstrumentation.attrMap(
                BullMQAttributes.JOB_OPTS,
                job.opts,
              ),
              [SemanticAttributes.MESSAGING_DESTINATION]: job.queueName,
              [BullMQAttributes.WORKER_CONCURRENCY]: this.opts?.concurrency,
              [BullMQAttributes.WORKER_LOCK_DURATION]: this.opts?.lockDuration,
              [BullMQAttributes.WORKER_LOCK_RENEW]: this.opts?.lockRenewTime,
              [BullMQAttributes.WORKER_RATE_LIMIT_MAX]: this.opts?.limiter?.max,
              [BullMQAttributes.WORKER_RATE_LIMIT_DURATION]:
                this.opts?.limiter?.duration,
              // Limit by group keys was removed in bullmq 3.x
              [BullMQAttributes.WORKER_RATE_LIMIT_GROUP]: (
                this.opts?.limiter as any
              )?.groupKey,
            }),
            kind: SpanKind.CONSUMER,
            links,
          },
          parentContext,
        );

        const consumerContext = trace.setSpan(parentContext, span);

        return await context.with(consumerContext, async () => {
          try {
            const result = await original.apply(this, [job, ...rest]);
            return result;
          } catch (e) {
            throw BullMQInstrumentation.setError(span, e as Error);
          } finally {
            span.setAttributes(
              BullMQInstrumentation.dropInvalidAttributes({
                [BullMQAttributes.JOB_FINISHED_TIMESTAMP]: job.finishedOn,
                [BullMQAttributes.JOB_PROCESSED_TIMESTAMP]: job.processedOn,
                [BullMQAttributes.JOB_FAILED_REASON]: job.failedReason,
              }),
            );

            span.end();
          }
        });
      };
    };
  }

  private _patchExtendLock(): (original: Fn) => (...args: any) => any {
    return function extendLock<T extends Fn>(original: T) {
      return function patch(this: Job, ...args: any): Promise<ReturnType<T>> {
        const span = trace.getSpan(context.active());
        span?.addEvent(
          "extendLock",
          BullMQInstrumentation.dropInvalidAttributes({
            [BullMQAttributes.JOB_NAME]: this.name,
            [BullMQAttributes.JOB_TIMESTAMP]: this.timestamp,
            [BullMQAttributes.JOB_PROCESSED_TIMESTAMP]: this.processedOn,
            [BullMQAttributes.JOB_ATTEMPTS]: this.attemptsMade,
          }),
        );

        return original.apply(this, args);
      };
    };
  }

  private _patchRemove(): (original: Fn) => (...args: any) => any {
    return function extendLock<T extends Fn>(original: T) {
      return function patch(this: Job, ...args: any): Promise<ReturnType<T>> {
        const span = trace.getSpan(context.active());
        span?.addEvent(
          "remove",
          BullMQInstrumentation.dropInvalidAttributes({
            [BullMQAttributes.JOB_NAME]: this.name,
            [BullMQAttributes.JOB_TIMESTAMP]: this.timestamp,
            [BullMQAttributes.JOB_PROCESSED_TIMESTAMP]: this.processedOn,
            [BullMQAttributes.JOB_ATTEMPTS]: this.attemptsMade,
          }),
        );

        return original.apply(this, args);
      };
    };
  }

  private _patchRetry(): (original: Fn) => (...args: any) => any {
    return function extendLock<T extends Fn>(original: T) {
      return function patch(this: Job, ...args: any): Promise<ReturnType<T>> {
        const span = trace.getSpan(context.active());
        span?.addEvent(
          "retry",
          BullMQInstrumentation.dropInvalidAttributes({
            [BullMQAttributes.JOB_NAME]: this.name,
            [BullMQAttributes.JOB_TIMESTAMP]: this.timestamp,
            [BullMQAttributes.JOB_PROCESSED_TIMESTAMP]: this.processedOn,
            [BullMQAttributes.JOB_ATTEMPTS]: this.attemptsMade,
          }),
        );

        return original.apply(this, args);
      };
    };
  }

  private configFor<K extends keyof BullMQInstrumentationConfig>(
    key: K,
  ): Required<BullMQInstrumentationConfig>[K] {
    return this._config[key] ?? defaultConfig[key];
  }

  // Return whether, according to the configuration, a span should be created
  // for each job enqueued by the given kind (bulk, flow, both or neither)
  // of operation.
  private shouldCreateSpan({
    isBulk,
    isFlow,
  }: {
    isBulk: boolean;
    isFlow: boolean;
  }): boolean {
    if (isBulk && isFlow) {
      return (
        this.configFor("emitCreateSpansForBulk") &&
        this.configFor("emitCreateSpansForFlow")
      );
    } else if (isBulk) {
      return this.configFor("emitCreateSpansForBulk");
    } else if (isFlow) {
      return this.configFor("emitCreateSpansForFlow");
    } else {
      return true;
    }
  }

  private static setError = (span: Span, error: Error) => {
    span.recordException(error);
    span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
    return error;
  };

  private static attrMap(prefix: string, opts: JobsOptions): Attributes {
    const attrs = flatten({ [prefix]: opts }) as Attributes;
    return this.dropInvalidAttributes(attrs);
  }

  private static async withContext(
    thisArg: any,
    original: Function,
    span: Span,
    args: any[],
    contextValues: Record<symbol, unknown> = {},
  ): Promise<any> {
    const parentContext = context.active();
    let messageContext = trace.setSpan(parentContext, span);

    for (const key of Object.getOwnPropertySymbols(contextValues)) {
      messageContext = messageContext.setValue(key, contextValues[key]);
    }

    return await context.with(messageContext, async () => {
      try {
        return await original.apply(thisArg, ...[args]);
      } catch (e) {
        throw BullMQInstrumentation.setError(span, e as Error);
      } finally {
        span.end();
      }
    });
  }

  private static dropInvalidAttributes(attributes: Attributes): Attributes {
    const keys = Object.keys(attributes);
    for (const key of keys) {
      if (attributes[key] === undefined || attributes[key] === null) {
        delete attributes[key];
      }
    }

    return attributes;
  }

  private static dropInvalidLinks(links: Partial<Link>[]): Link[] {
    return links.filter((link) => link.context !== undefined) as Link[];
  }
}
