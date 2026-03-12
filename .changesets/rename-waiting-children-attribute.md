---
bump: minor
type: change
---

Replace the `messaging.bullmq.job.parentOpts.waitChildrenKey` span attribute with `messaging.bullmq.job.parentOpts.addToWaitingChildren`. The old attribute reported an internal Redis key string (`bull:<queue>:waiting-children`) that bullmq exposed up to version 5.61.2. From 5.62.0, bullmq replaced it with an `addToWaitingChildren` boolean flag. The new attribute normalises both representations into a single boolean: `true` when a flow parent job is waiting for its children to complete.
