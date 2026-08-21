---
bump: minor
type: change
---

Declare `@opentelemetry/api` as a peer dependency instead of a regular
dependency, so that npm installs one copy of it, shared with your application.

Two copies of `@opentelemetry/api` in the same project can silently stop this
instrumentation from reporting anything. The OpenTelemetry API keeps its global
tracer provider on a global object, and a copy of the API only reads that global
when its version is compatible with the version that wrote it. When the copies
do not match, this instrumentation receives a tracer that does nothing, and no
error is reported.

If your project pins `@opentelemetry/api` to a version outside the range this
package supports, npm will now warn you about it at install time rather than
installing a second copy.
