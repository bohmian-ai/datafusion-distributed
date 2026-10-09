# Releasing worker streams

A distributed plan reads from workers through network streams that buffer
incoming batches. Those streams are owned by the query, not by the operator
reading them: when the query's output stream ends, fails, or is dropped, every
worker stream it opened is dropped too, along with the reader tasks and buffered
batches behind it, and the remote work feeding them is cancelled. An operator
that stops polling one of its inputs early, such as a join whose build side is
done, does not keep that input's memory reserved past the end of the query.

The release happens in the background. To wait for it, for example before
asserting on a `MemoryPool` or reusing a memory-constrained runtime, call
`DistributedExec::wait_closed`:

```rust
use datafusion_distributed::DistributedExec;

let plan = ctx.sql(sql).await?.create_physical_plan().await?;
let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;

if let Some(plan) = plan.downcast_ref::<DistributedExec>() {
    plan.wait_closed().await;
}
assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
```

`wait_closed` covers the streams opened by the last `execute()` call on that
plan, and returns immediately if it was never executed.

Workers do the same for every task they execute: a task's streams are released
once all of its output partitions have been consumed, failed, or dropped, or
when the coordinator ends the query.
