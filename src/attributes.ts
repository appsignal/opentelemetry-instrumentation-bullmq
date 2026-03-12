import {
  ATTR_MESSAGING_SYSTEM,
  ATTR_MESSAGING_DESTINATION_NAME,
  ATTR_MESSAGING_OPERATION_NAME,
  ATTR_MESSAGING_OPERATION_TYPE,
  ATTR_MESSAGING_MESSAGE_ID,
  ATTR_MESSAGING_CLIENT_ID,
  ATTR_MESSAGING_BATCH_MESSAGE_COUNT,
  MESSAGING_OPERATION_TYPE_VALUE_SEND,
  MESSAGING_OPERATION_TYPE_VALUE_CREATE,
  MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
} from "@opentelemetry/semantic-conventions/incubating";

const ns = "messaging.bullmq";
const job = `${ns}.job`;
const queue = `${ns}.queue`;
const worker = `${ns}.worker`;

export const BullMQAttributes = {
  MESSAGING_SYSTEM: "bullmq",
  JOB_ATTEMPTS: `${job}.attempts`,
  JOB_DELAY: `${job}.delay`,
  JOB_FAILED_REASON: `${job}.failedReason`,
  JOB_FINISHED_TIMESTAMP: `${job}.finishedOn`,
  JOB_PROCESSED_TIMESTAMP: `${job}.processedOn`,
  JOB_NAME: `${job}.name`,
  JOB_OPTS: `${job}.opts`,
  JOB_REPEAT_KEY: `${job}.repeatJobKey`,
  JOB_TIMESTAMP: `${job}.timestamp`,
  JOB_PARENT_KEY: `${job}.parentOpts.parentKey`,
  JOB_ADD_TO_WAITING_CHILDREN: `${job}.parentOpts.addToWaitingChildren`,
  JOB_BULK_NAMES: `${job}.bulk.names`,
  WORKER_CONCURRENCY: `${worker}.concurrency`,
  WORKER_LOCK_DURATION: `${worker}.lockDuration`,
  WORKER_LOCK_RENEW: `${worker}.lockRenewTime`,
  WORKER_RATE_LIMIT_MAX: `${worker}.rateLimiter.max`,
  WORKER_RATE_LIMIT_DURATION: `${worker}.rateLimiter.duration`,
  WORKER_RATE_LIMIT_GROUP: `${worker}.rateLimiter.groupKey`,
};

export const SemanticAttributes = {
  MESSAGING_SYSTEM: ATTR_MESSAGING_SYSTEM,
  MESSAGING_DESTINATION_NAME: ATTR_MESSAGING_DESTINATION_NAME,
  MESSAGING_OPERATION_NAME: ATTR_MESSAGING_OPERATION_NAME,
  MESSAGING_OPERATION_TYPE: ATTR_MESSAGING_OPERATION_TYPE,
  MESSAGING_MESSAGE_ID: ATTR_MESSAGING_MESSAGE_ID,
  MESSAGING_CLIENT_ID: ATTR_MESSAGING_CLIENT_ID,
  MESSAGING_BATCH_MESSAGE_COUNT: ATTR_MESSAGING_BATCH_MESSAGE_COUNT,
};

export const MessagingOperationTypeValues = {
  SEND: MESSAGING_OPERATION_TYPE_VALUE_SEND,
  CREATE: MESSAGING_OPERATION_TYPE_VALUE_CREATE,
  PROCESS: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
};
