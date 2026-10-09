use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::Poll;
use tokio_util::sync::CancellationToken;
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};

/// A worker partition stream that the [StreamCloseState] it was registered with can drop at any
/// time, whether or not it was already handed out to a consumer.
pub(crate) type StreamSlot = Mutex<Option<BoxStream<'static, Result<RecordBatch>>>>;

/// Releases every worker partition stream opened while executing one plan.
///
/// A plan reading from other workers opens one connection per input task, each one buffering the
/// batches of a range of partitions until they are consumed. A consumer that stops polling one of
/// them early, or a partition nobody executes, would otherwise keep the connection, its reader
/// task, and its buffered batches alive for as long as the plan lives.
///
/// One instance is created per [crate::DistributedExec] execution in the coordinator, and per
/// executed task in a worker, and is reachable through the [TaskContext] session config. Closing
/// it drops every stream registered with it, which cancels the remote work feeding them.
#[derive(Debug, Default)]
pub(crate) struct StreamCloseState {
    closed: CancellationToken,
    streams: Mutex<Vec<Weak<StreamSlot>>>,
    readers: TaskTracker,
    finished_outputs: AtomicUsize,
}

impl StreamCloseState {
    /// Returns the [StreamCloseState] present in the provided [TaskContext], if any.
    pub(crate) fn from_ctx(ctx: &TaskContext) -> Option<Arc<Self>> {
        ctx.session_config().get_extension::<Self>()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
    }

    /// Returns a token that keeps [Self::wait_closed] pending until it is dropped. Held by
    /// in-flight connections, network reader tasks, and the buffers they fill.
    pub(crate) fn track(&self) -> TaskTrackerToken {
        self.readers.token()
    }

    /// Registers streams that must be dropped on [Self::close]. If already closed, they are dropped
    /// right away.
    pub(crate) fn register<'a>(&self, slots: impl IntoIterator<Item = &'a Arc<StreamSlot>>) {
        let mut streams = self.streams.lock().expect("poisoned lock");
        if !self.is_closed() {
            streams.extend(slots.into_iter().map(Arc::downgrade));
            return;
        }
        drop(streams);
        for slot in slots {
            let stream = slot.lock().expect("poisoned lock").take();
            drop(stream);
        }
    }

    /// Closes this state, dropping every registered stream. Idempotent.
    pub(crate) fn close(&self) {
        self.closed.cancel();
        self.readers.close();
        let streams = std::mem::take(&mut *self.streams.lock().expect("poisoned lock"));
        for slot in streams.iter().filter_map(Weak::upgrade) {
            let stream = slot.lock().expect("poisoned lock").take();
            drop(stream);
        }
    }

    /// Resolves once this state is closed and every tracked connection, reader task and buffer
    /// has been dropped.
    pub(crate) async fn wait_closed(&self) {
        self.readers.wait().await
    }

    /// Returns a stream that polls the stream in `slot`, ending as soon as the slot is emptied.
    pub(crate) fn slot_stream(slot: Arc<StreamSlot>) -> BoxStream<'static, Result<RecordBatch>> {
        stream::poll_fn(
            move |cx| match slot.lock().expect("poisoned lock").as_mut() {
                Some(stream) => stream.poll_next_unpin(cx),
                None => Poll::Ready(None),
            },
        )
        .boxed()
    }

    /// Wraps one of the `total` output partition streams of a worker task. This state is closed
    /// once every output stream ended or got dropped, or as soon as one of them fails.
    pub(crate) fn track_output(
        self: &Arc<Self>,
        mut stream: SendableRecordBatchStream,
        total: usize,
    ) -> SendableRecordBatchStream {
        let schema = stream.schema();
        let mut output = Some(TrackedOutput {
            state: Arc::clone(self),
            total,
            _token: self.track(),
        });
        let stream = stream::poll_fn(move |cx| {
            let poll = stream.poll_next_unpin(cx);
            match &poll {
                Poll::Ready(Some(Err(_))) => {
                    if let Some(output) = output.take() {
                        output.state.close();
                    }
                }
                Poll::Ready(None) => drop(output.take()),
                _ => {}
            }
            poll
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

/// Closes the [StreamCloseState] it holds when dropped.
pub(crate) struct CloseOnDrop(pub(crate) Arc<StreamCloseState>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// One output partition stream of a worker task, counted as finished when dropped.
struct TrackedOutput {
    state: Arc<StreamCloseState>,
    total: usize,
    _token: TaskTrackerToken,
}

impl Drop for TrackedOutput {
    fn drop(&mut self) {
        if self.state.finished_outputs.fetch_add(1, Ordering::SeqCst) + 1 >= self.total {
            self.state.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::in_memory_channel_resolver::{
        InMemoryChannelResolver, InMemoryWorkerResolver, start_configured_in_memory_context,
    };
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::{
        ChannelResolver, CoordinatorToWorkerMsg, DefaultSessionBuilder, DistributedExec,
        DistributedExt, ExecuteTaskRequest, GetWorkerInfoRequest, GetWorkerInfoResponse,
        SessionStateBuilderExt, SetPlanRequest, WorkerChannel, WorkerQueryContext,
        WorkerSessionBuilder, WorkerToCoordinatorMsg, display_plan_ascii,
    };
    use async_trait::async_trait;
    use datafusion::error::DataFusionError;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::FutureExt;
    use http::HeaderMap;
    use std::future::Future;
    use std::time::Duration;
    use tokio::sync::{Notify, Semaphore};
    use url::Url;

    #[tokio::test]
    async fn drops_streams_of_a_connection_resolving_after_close() {
        let channel = GatedChannel::default();
        let ctx = gated_context(channel.clone()).await;
        let plan = plan(&ctx, "SELECT * FROM weather").await;

        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        within(channel.entered.notified()).await;
        let state = channel.state.lock().unwrap().take().unwrap();
        drop(stream);
        within(state.closed.cancelled()).await;
        // The in-flight connection keeps the query from being reported as released.
        assert!(state.wait_closed().now_or_never().is_none());

        channel.open.add_permits(Semaphore::MAX_PERMITS);
        within(dist(&plan).wait_closed()).await;
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn closes_worker_tasks_when_the_query_is_dropped() {
        let tasks = Arc::new(Mutex::new(vec![]));
        let worker_runtime = Arc::new(RuntimeEnv::default());
        let runtime = Arc::clone(&worker_runtime);
        let ctx =
            start_configured_in_memory_context(3, RecordTasks(Arc::clone(&tasks)), move |w| {
                w.with_runtime_env(Arc::clone(&runtime))
            })
            .await;
        register_parquet_tables(&ctx).await.unwrap();
        let plan = plan(
            &ctx,
            r#"SELECT "MinTemp", count(*) FROM weather GROUP BY "MinTemp""#,
        )
        .await;
        let display = display_plan_ascii(plan.as_ref(), false);
        assert!(display.contains("NetworkShuffleExec"), "{display}");

        let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
        stream.next().await.unwrap().unwrap();
        drop(stream);

        within(dist(&plan).wait_closed()).await;
        let tasks = std::mem::take(&mut *tasks.lock().unwrap());
        assert!(!tasks.is_empty());
        for task in tasks {
            within(task.wait_closed()).await;
            assert!(task.is_closed());
        }
        assert_eq!(worker_runtime.memory_pool.reserved(), 0);
    }

    async fn plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(plan.is::<DistributedExec>());
        plan
    }

    fn dist(plan: &Arc<dyn ExecutionPlan>) -> &DistributedExec {
        plan.downcast_ref::<DistributedExec>().unwrap()
    }

    async fn within<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .expect("timed out")
    }

    /// Records the [StreamCloseState] of every task the workers build a session for.
    struct RecordTasks(Arc<Mutex<Vec<Arc<StreamCloseState>>>>);

    #[async_trait]
    impl WorkerSessionBuilder for RecordTasks {
        async fn build_session_state(
            &self,
            ctx: WorkerQueryContext,
        ) -> Result<SessionState, DataFusionError> {
            let state = ctx.builder.build();
            let close_state = state.config().get_extension::<StreamCloseState>();
            self.0.lock().unwrap().extend(close_state);
            Ok(state)
        }
    }

    async fn gated_context(channel: GatedChannel) -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(3))
            .with_distributed_planner()
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
            .with_distributed_channel_resolver(channel)
            .with_distributed_file_scan_config_bytes_per_partition(1)
            .unwrap()
            .build();
        let ctx = SessionContext::from(state);
        register_parquet_tables(&ctx).await.unwrap();
        ctx
    }

    /// In-memory workers whose connections wait for `open` before executing tasks, recording
    /// the coordinator's [StreamCloseState] they were opened under.
    #[derive(Clone)]
    struct GatedChannel {
        inner: InMemoryChannelResolver,
        entered: Arc<Notify>,
        open: Arc<Semaphore>,
        state: Arc<Mutex<Option<Arc<StreamCloseState>>>>,
    }

    impl Default for GatedChannel {
        fn default() -> Self {
            Self {
                inner: InMemoryChannelResolver::from_session_builder(DefaultSessionBuilder),
                entered: Arc::default(),
                open: Arc::new(Semaphore::new(0)),
                state: Arc::default(),
            }
        }
    }

    #[async_trait]
    impl ChannelResolver for GatedChannel {
        async fn get_worker_client_for_url(
            &self,
            url: &Url,
        ) -> Result<Box<dyn WorkerChannel>, DataFusionError> {
            Ok(Box::new(GatedWorkerChannel {
                inner: self.inner.get_worker_client_for_url(url).await?,
                gate: self.clone(),
            }))
        }
    }

    struct GatedWorkerChannel {
        inner: Box<dyn WorkerChannel>,
        gate: GatedChannel,
    }

    #[async_trait]
    impl WorkerChannel for GatedWorkerChannel {
        async fn coordinator_channel(
            &mut self,
            headers: HeaderMap,
            set_plan_request: SetPlanRequest,
            c2w_stream: BoxStream<'static, CoordinatorToWorkerMsg>,
            metrics: ExecutionPlanMetricsSet,
            task_ctx: &Arc<TaskContext>,
        ) -> Result<BoxStream<'static, Result<WorkerToCoordinatorMsg>>> {
            self.inner
                .coordinator_channel(headers, set_plan_request, c2w_stream, metrics, task_ctx)
                .await
        }

        async fn execute_task(
            &mut self,
            headers: HeaderMap,
            request: ExecuteTaskRequest,
            metrics: ExecutionPlanMetricsSet,
            task_ctx: &Arc<TaskContext>,
        ) -> Result<Vec<BoxStream<'static, Result<RecordBatch>>>> {
            *self.gate.state.lock().unwrap() = StreamCloseState::from_ctx(task_ctx);
            self.gate.entered.notify_one();
            drop(self.gate.open.acquire().await);
            self.inner
                .execute_task(headers, request, metrics, task_ctx)
                .await
        }

        async fn get_worker_info(
            &mut self,
            request: GetWorkerInfoRequest,
        ) -> Result<GetWorkerInfoResponse> {
            self.inner.get_worker_info(request).await
        }
    }
}
