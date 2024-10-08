---
bump: patch
type: add
---

Add a `useProducerSpanAsConsumerParent` configuration option that defaults to `false`. When set to `true`, instead of establishing a span link from the consumer span to the producer span, the consumer span will be in the same trace, as a child span of the producer span.
