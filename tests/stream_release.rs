#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::Result;
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DefaultSessionBuilder, DistributedExec, NetworkBoundaryExt};
    use futures::{StreamExt, TryStreamExt, stream};
    use std::fmt::{Debug, Formatter};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const WEATHER_ROWS: usize = 366;

    #[tokio::test]
    async fn releases_streams_left_unread_after_the_query_ends() -> Result<()> {
        let cluster = Cluster::new().await?;
        let held = Arc::new(Mutex::new(vec![]));
        let plan = hold_network_inputs(cluster.plan("SELECT * FROM weather").await?, &held)?;

        assert_eq!(cluster.run(&plan).await?, 0);
        cluster.assert_released(&plan).await;

        // The streams the plan never finished reading were cut off when the query ended.
        let held = std::mem::take(&mut *held.lock().unwrap());
        assert!(!held.is_empty());
        for mut stream in held {
            assert!(stream.next().await.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn releases_streams_when_dropped_mid_query() -> Result<()> {
        let cluster = Cluster::new().await?;
        let plan = cluster.plan("SELECT * FROM weather").await?;

        let mut stream = plan.execute(0, cluster.ctx.task_ctx())?;
        stream.next().await.unwrap()?;
        drop(stream);

        cluster.assert_released(&plan).await;
        Ok(())
    }

    #[tokio::test]
    async fn releases_streams_when_dropped_before_polled() -> Result<()> {
        let cluster = Cluster::new().await?;
        let plan = cluster.plan("SELECT * FROM weather").await?;

        drop(plan.execute(0, cluster.ctx.task_ctx())?);

        cluster.assert_released(&plan).await;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_one_query_leaves_a_concurrent_one_running() -> Result<()> {
        let cluster = Cluster::new().await?;
        let dropped = cluster.plan("SELECT * FROM weather").await?;
        let running = cluster.plan("SELECT * FROM weather").await?;

        let mut dropped_stream = dropped.execute(0, cluster.ctx.task_ctx())?;
        let mut running_stream = running.execute(0, cluster.ctx.task_ctx())?;
        dropped_stream.next().await.unwrap()?;
        let first = running_stream.next().await.unwrap()?;
        drop(dropped_stream);
        within(dist(&dropped).wait_closed()).await;

        let rest = running_stream.try_collect::<Vec<_>>().await?;
        let rows = first.num_rows() + rest.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(rows, WEATHER_ROWS);
        cluster.assert_released(&running).await;
        Ok(())
    }

    /// A coordinator backed by in-memory workers that share one [RuntimeEnv].
    struct Cluster {
        ctx: SessionContext,
        worker_runtime: Arc<RuntimeEnv>,
    }

    impl Cluster {
        async fn new() -> Result<Self> {
            let worker_runtime = Arc::new(RuntimeEnv::default());
            let runtime = Arc::clone(&worker_runtime);
            let ctx = start_configured_in_memory_context(3, DefaultSessionBuilder, move |w| {
                w.with_runtime_env(Arc::clone(&runtime))
            })
            .await;
            register_parquet_tables(&ctx).await?;
            Ok(Self {
                ctx,
                worker_runtime,
            })
        }

        async fn plan(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
            let plan = self.ctx.sql(sql).await?.create_physical_plan().await?;
            assert!(plan.is::<DistributedExec>());
            Ok(plan)
        }

        /// Executes `plan` to completion, returning its row count.
        async fn run(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<usize> {
            let batches = plan
                .execute(0, self.ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?;
            Ok(batches.iter().map(|b| b.num_rows()).sum())
        }

        /// Waits for `plan` to release its worker streams, then asserts no memory is reserved.
        async fn assert_released(&self, plan: &Arc<dyn ExecutionPlan>) {
            within(dist(plan).wait_closed()).await;
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

    /// Places a [HoldInputExec] on top of every network boundary of the coordinator's stage.
    fn hold_network_inputs(
        plan: Arc<dyn ExecutionPlan>,
        held: &Arc<Mutex<Vec<SendableRecordBatchStream>>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan.transform_down(|node| {
            if !node.is_network_boundary() {
                return Ok(Transformed::no(node));
            }
            let node = Arc::new(HoldInputExec {
                input: node,
                held: Arc::clone(held),
            });
            Ok(Transformed::new(node, true, TreeNodeRecursion::Jump))
        })?;
        Ok(plan.data)
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
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                input: children.swap_remove(0),
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
