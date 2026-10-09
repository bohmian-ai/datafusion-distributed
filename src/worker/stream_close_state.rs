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
    use crate::common::require_one_child;
    use crate::test_utils::in_memory_channel_resolver::{
        InMemoryChannelResolver, InMemoryWorkerResolver,
    };
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::{
        ChannelResolver, CoordinatorToWorkerMsg, DistributedExec, DistributedExt,
        ExecuteTaskRequest, GetWorkerInfoRequest, GetWorkerInfoResponse, NetworkBoundaryExt,
        SessionStateBuilderExt, SetPlanRequest, WorkerChannel, WorkerQueryContext,
        WorkerSessionBuilder, WorkerToCoordinatorMsg, display_plan_ascii,
    };
    use async_trait::async_trait;
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    use datafusion::error::DataFusionError;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::{FutureExt, TryStreamExt};
    use http::HeaderMap;
    use std::fmt::{Debug, Formatter};
    use std::future::Future;
    use std::time::Duration;
    use tokio::sync::{Notify, Semaphore};
    use url::Url;

    const WEATHER_ROWS: usize = 366;

    #[tokio::test]
    async fn releases_held_build_side_after_end_of_stream() {
        let cluster = Cluster::new(true).await;
        let held = Arc::new(Mutex::new(vec![]));
        let plan = hold_network_inputs(cluster.plan("SELECT * FROM weather").await, &held);

        assert_eq!(cluster.run(&plan).await, 0);
        cluster.assert_released(&plan).await;

        // The pools dropped what the held streams were reading, so they end when polled.
        let held = std::mem::take(&mut *held.lock().unwrap());
        assert!(!held.is_empty());
        for mut stream in held {
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn releases_streams_when_root_is_dropped_mid_query() {
        let cluster = Cluster::new(true).await;
        let plan = cluster.plan("SELECT * FROM weather").await;

        let mut stream = plan.execute(0, cluster.ctx.task_ctx()).unwrap();
        stream.next().await.unwrap().unwrap();
        drop(stream);

        cluster.assert_released(&plan).await;
    }

    #[tokio::test]
    async fn releases_streams_when_root_is_dropped_before_preparation() {
        let cluster = Cluster::new(true).await;
        let plan = cluster.plan("SELECT * FROM weather").await;

        drop(plan.execute(0, cluster.ctx.task_ctx()).unwrap());

        cluster.assert_released(&plan).await;
    }

    #[tokio::test]
    async fn drops_streams_of_a_connection_resolving_after_close() {
        let cluster = Cluster::new(false).await;
        let plan = cluster.plan("SELECT * FROM weather").await;

        let stream = plan.execute(0, cluster.ctx.task_ctx()).unwrap();
        within(cluster.channel.gate.entered.notified()).await;
        let state = Arc::clone(&cluster.channel.states.lock().unwrap()[0]);
        drop(stream);
        within(state.closed.cancelled()).await;
        // The in-flight connection keeps the query from being reported as released.
        assert!(state.wait_closed().now_or_never().is_none());

        cluster.channel.gate.open.add_permits(1);
        cluster.assert_released(&plan).await;
    }

    #[tokio::test]
    async fn closing_one_query_leaves_a_concurrent_one_running() {
        let cluster = Cluster::new(true).await;
        let closed = cluster.plan("SELECT * FROM weather").await;
        let running = cluster.plan("SELECT * FROM weather").await;

        let mut closed_stream = closed.execute(0, cluster.ctx.task_ctx()).unwrap();
        let mut running_stream = running.execute(0, cluster.ctx.task_ctx()).unwrap();
        closed_stream.next().await.unwrap().unwrap();
        let first = running_stream.next().await.unwrap().unwrap();
        drop(closed_stream);
        within(dist(&closed).wait_closed()).await;

        let rest: usize = running_stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        assert_eq!(first.num_rows() + rest, WEATHER_ROWS);
        cluster.assert_released(&running).await;
    }

    #[tokio::test]
    async fn releases_worker_stage_streams_when_root_is_dropped() {
        let cluster = Cluster::new(true).await;
        let sql = r#"SELECT "MinTemp", count(*) FROM weather GROUP BY "MinTemp""#;
        let plan = cluster.plan(sql).await;
        let display = display_plan_ascii(plan.as_ref(), false);
        assert!(display.contains("NetworkShuffleExec"), "{display}");

        let mut stream = plan.execute(0, cluster.ctx.task_ctx()).unwrap();
        stream.next().await.unwrap().unwrap();
        drop(stream);
        cluster.assert_released(&plan).await;

        let sibling = cluster.plan("SELECT * FROM weather").await;
        assert_eq!(cluster.run(&sibling).await, WEATHER_ROWS);
        cluster.assert_released(&sibling).await;
    }

    /// A coordinator backed by in-memory workers, recording the [StreamCloseState] of every
    /// coordinator query and worker task.
    struct Cluster {
        ctx: SessionContext,
        channel: RecordingChannel,
        worker_tasks: Arc<Mutex<Vec<Arc<StreamCloseState>>>>,
        worker_runtime: Arc<RuntimeEnv>,
    }

    impl Cluster {
        /// Builds the cluster. When `open` is false, coordinator connections to workers wait
        /// until [Gate::open] gets a permit.
        async fn new(open: bool) -> Self {
            let worker_tasks = Arc::new(Mutex::new(vec![]));
            let worker_runtime = Arc::new(RuntimeEnv::default());
            let runtime = Arc::clone(&worker_runtime);
            let inner = InMemoryChannelResolver::from_configured_worker(
                RecordWorkerTasks(Arc::clone(&worker_tasks)),
                move |worker| worker.with_runtime_env(Arc::clone(&runtime)),
            );
            let channel = RecordingChannel {
                inner,
                gate: Arc::new(Gate {
                    entered: Notify::new(),
                    open: Semaphore::new(if open { Semaphore::MAX_PERMITS } else { 0 }),
                }),
                states: Arc::new(Mutex::new(vec![])),
            };
            let state = SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(3))
                .with_distributed_planner()
                .with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
                .with_distributed_channel_resolver(channel.clone())
                .with_distributed_file_scan_config_bytes_per_partition(1)
                .unwrap()
                .with_distributed_shuffle_batch_size(8)
                .unwrap()
                .build();
            let ctx = SessionContext::from(state);
            register_parquet_tables(&ctx).await.unwrap();
            Self {
                ctx,
                channel,
                worker_tasks,
                worker_runtime,
            }
        }

        async fn plan(&self, sql: &str) -> Arc<dyn ExecutionPlan> {
            let df = self.ctx.sql(sql).await.unwrap();
            let plan = df.create_physical_plan().await.unwrap();
            assert!(plan.is::<DistributedExec>());
            plan
        }

        /// Executes `plan` to completion, returning its row count.
        async fn run(&self, plan: &Arc<dyn ExecutionPlan>) -> usize {
            let stream = plan.execute(0, self.ctx.task_ctx()).unwrap();
            let batches = stream.try_collect::<Vec<_>>().await.unwrap();
            batches.iter().map(|batch| batch.num_rows()).sum()
        }

        /// Asserts that every stream opened so far was released: each recorded close state is
        /// closed with no reader task left, and no memory pool holds a reservation.
        async fn assert_released(&self, plan: &Arc<dyn ExecutionPlan>) {
            within(dist(plan).wait_closed()).await;
            let mut states = std::mem::take(&mut *self.channel.states.lock().unwrap());
            states.append(&mut self.worker_tasks.lock().unwrap());
            for state in states {
                within(state.wait_closed()).await;
                assert!(state.is_closed());
                assert!(state.readers.is_empty());
            }
            assert_eq!(self.ctx.runtime_env().memory_pool.reserved(), 0);
            assert_eq!(self.worker_runtime.memory_pool.reserved(), 0);
        }
    }

    fn dist(plan: &Arc<dyn ExecutionPlan>) -> &DistributedExec {
        plan.downcast_ref::<DistributedExec>().unwrap()
    }

    async fn within<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .expect("timed out")
    }

    /// Records the [StreamCloseState] of every task the worker builds a session for.
    struct RecordWorkerTasks(Arc<Mutex<Vec<Arc<StreamCloseState>>>>);

    #[async_trait]
    impl WorkerSessionBuilder for RecordWorkerTasks {
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

    /// Blocks coordinator connections to workers until opened.
    struct Gate {
        entered: Notify,
        open: Semaphore,
    }

    /// Wraps the coordinator's channel resolver, recording the [StreamCloseState] each worker
    /// connection is opened under, and making connections wait on the [Gate].
    #[derive(Clone)]
    struct RecordingChannel {
        inner: InMemoryChannelResolver,
        gate: Arc<Gate>,
        states: Arc<Mutex<Vec<Arc<StreamCloseState>>>>,
    }

    #[async_trait]
    impl ChannelResolver for RecordingChannel {
        async fn get_worker_client_for_url(
            &self,
            url: &Url,
        ) -> Result<Box<dyn WorkerChannel>, DataFusionError> {
            Ok(Box::new(RecordingWorkerChannel {
                inner: self.inner.get_worker_client_for_url(url).await?,
                channel: self.clone(),
            }))
        }
    }

    struct RecordingWorkerChannel {
        inner: Box<dyn WorkerChannel>,
        channel: RecordingChannel,
    }

    #[async_trait]
    impl WorkerChannel for RecordingWorkerChannel {
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
            let state = StreamCloseState::from_ctx(task_ctx);
            self.channel.states.lock().unwrap().extend(state);
            self.channel.gate.entered.notify_one();
            drop(self.channel.gate.open.acquire().await);
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

    /// Places a [HoldInputExec] on top of every network boundary of the coordinator's stage.
    fn hold_network_inputs(
        plan: Arc<dyn ExecutionPlan>,
        held: &Arc<Mutex<Vec<SendableRecordBatchStream>>>,
    ) -> Arc<dyn ExecutionPlan> {
        plan.transform_down(|node| {
            if !node.is_network_boundary() {
                return Ok(Transformed::no(node));
            }
            let node = Arc::new(HoldInputExec {
                input: node,
                held: Arc::clone(held),
            });
            Ok(Transformed::new(node, true, TreeNodeRecursion::Jump))
        })
        .unwrap()
        .data
    }

    /// Stands in for a join's build side that stops being polled: reads the first batch of each
    /// input partition, then parks the still open stream in `held` and outputs nothing.
    struct HoldInputExec {
        input: Arc<dyn ExecutionPlan>,
        held: Arc<Mutex<Vec<SendableRecordBatchStream>>>,
    }

    impl Debug for HoldInputExec {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str("HoldInputExec")
        }
    }

    impl DisplayAs for HoldInputExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            f.write_str("HoldInputExec")
        }
    }

    impl ExecutionPlan for HoldInputExec {
        fn name(&self) -> &str {
            "HoldInputExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                input: require_one_child(children)?,
                held: Arc::clone(&self.held),
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let mut input = self.input.execute(partition, context)?;
            let held = Arc::clone(&self.held);
            let stream = stream::once(async move {
                input.next().await.transpose()?;
                held.lock().unwrap().push(input);
                Ok(())
            })
            .try_filter_map(|()| async { Ok(None) });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        }
    }
}
