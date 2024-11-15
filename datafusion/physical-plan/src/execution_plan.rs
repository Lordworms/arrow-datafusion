// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_array::Array;
use futures::stream::{StreamExt, TryStreamExt};
use tokio::task::JoinSet;

use datafusion_common::config::ConfigOptions;
pub use datafusion_common::hash_utils;
pub use datafusion_common::utils::project_schema;
use datafusion_common::{exec_err, Result};
pub use datafusion_common::{internal_err, ColumnStatistics, Statistics};
use datafusion_execution::TaskContext;
pub use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
pub use datafusion_expr::{Accumulator, ColumnarValue};
pub use datafusion_physical_expr::window::WindowExpr;
pub use datafusion_physical_expr::{
    expressions, udf, Distribution, Partitioning, PhysicalExpr,
};
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion_physical_expr_common::sort_expr::LexRequirement;

use crate::coalesce_partitions::CoalescePartitionsExec;
use crate::display::DisplayableExecutionPlan;
pub use crate::display::{DefaultDisplay, DisplayAs, DisplayFormatType, VerboseDisplay};
use crate::joins::DynamicFilterInfo;
pub use crate::metrics::Metric;
use crate::metrics::MetricsSet;
pub use crate::ordering::InputOrderMode;
use crate::repartition::RepartitionExec;
use crate::sorts::sort_preserving_merge::SortPreservingMergeExec;
pub use crate::stream::EmptyRecordBatchStream;
use crate::stream::RecordBatchStreamAdapter;

/// Represent nodes in the DataFusion Physical Plan.
///
/// Calling [`execute`] produces an `async` [`SendableRecordBatchStream`] of
/// [`RecordBatch`] that incrementally computes a partition of the
/// `ExecutionPlan`'s output from its input. See [`Partitioning`] for more
/// details on partitioning.
///
/// Methods such as [`Self::schema`] and [`Self::properties`] communicate
/// properties of the output to the DataFusion optimizer, and methods such as
/// [`required_input_distribution`] and [`required_input_ordering`] express
/// requirements of the `ExecutionPlan` from its input.
///
/// [`ExecutionPlan`] can be displayed in a simplified form using the
/// return value from [`displayable`] in addition to the (normally
/// quite verbose) `Debug` output.
///
/// [`execute`]: ExecutionPlan::execute
/// [`required_input_distribution`]: ExecutionPlan::required_input_distribution
/// [`required_input_ordering`]: ExecutionPlan::required_input_ordering
pub trait ExecutionPlan: Debug + DisplayAs + Send + Sync {
    /// Short name for the ExecutionPlan, such as 'ParquetExec'.
    ///
    /// Implementation note: this method can just proxy to
    /// [`static_name`](ExecutionPlan::static_name) if no special action is
    /// needed. It doesn't provide a default implementation like that because
    /// this method doesn't require the `Sized` constrain to allow a wilder
    /// range of use cases.
    fn name(&self) -> &str;

    /// Short name for the ExecutionPlan, such as 'ParquetExec'.
    /// Like [`name`](ExecutionPlan::name) but can be called without an instance.
    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        let full_name = std::any::type_name::<Self>();
        let maybe_start_idx = full_name.rfind(':');
        match maybe_start_idx {
            Some(start_idx) => &full_name[start_idx + 1..],
            None => "UNKNOWN",
        }
    }

    /// Returns the execution plan as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties().schema())
    }

    /// Return properties of the output of the `ExecutionPlan`, such as output
    /// ordering(s), partitioning information etc.
    ///
    /// This information is available via methods on [`ExecutionPlanProperties`]
    /// trait, which is implemented for all `ExecutionPlan`s.
    fn properties(&self) -> &PlanProperties;

    /// Specifies the data distribution requirements for all the
    /// children for this `ExecutionPlan`, By default it's [[Distribution::UnspecifiedDistribution]] for each child,
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    /// Specifies the ordering required for all of the children of this
    /// `ExecutionPlan`.
    ///
    /// For each child, it's the local ordering requirement within
    /// each partition rather than the global ordering
    ///
    /// NOTE that checking `!is_empty()` does **not** check for a
    /// required input ordering. Instead, the correct check is that at
    /// least one entry must be `Some`
    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        vec![None; self.children().len()]
    }

    /// Returns `false` if this `ExecutionPlan`'s implementation may reorder
    /// rows within or between partitions.
    ///
    /// For example, Projection, Filter, and Limit maintain the order
    /// of inputs -- they may transform values (Projection) or not
    /// produce the same number of rows that went in (Filter and
    /// Limit), but the rows that are produced go in the same way.
    ///
    /// DataFusion uses this metadata to apply certain optimizations
    /// such as automatically repartitioning correctly.
    ///
    /// The default implementation returns `false`
    ///
    /// WARNING: if you override this default, you *MUST* ensure that
    /// the `ExecutionPlan`'s maintains the ordering invariant or else
    /// DataFusion may produce incorrect results.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    /// Specifies whether the `ExecutionPlan` benefits from increased
    /// parallelization at its input for each child.
    ///
    /// If returns `true`, the `ExecutionPlan` would benefit from partitioning
    /// its corresponding child (and thus from more parallelism). For
    /// `ExecutionPlan` that do very little work the overhead of extra
    /// parallelism may outweigh any benefits
    ///
    /// The default implementation returns `true` unless this `ExecutionPlan`
    /// has signalled it requires a single child input partition.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // By default try to maximize parallelism with more CPUs if
        // possible
        self.required_input_distribution()
            .into_iter()
            .map(|dist| !matches!(dist, Distribution::SinglePartition))
            .collect()
    }

    /// Get a list of children `ExecutionPlan`s that act as inputs to this plan.
    /// The returned list will be empty for leaf nodes such as scans, will contain
    /// a single value for unary nodes, or two values for binary nodes (such as
    /// joins).
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;

    /// Returns a new `ExecutionPlan` where all existing children were replaced
    /// by the `children`, in order
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// If supported, attempt to increase the partitioning of this `ExecutionPlan` to
    /// produce `target_partitions` partitions.
    ///
    /// If the `ExecutionPlan` does not support changing its partitioning,
    /// returns `Ok(None)` (the default).
    ///
    /// It is the `ExecutionPlan` can increase its partitioning, but not to the
    /// `target_partitions`, it may return an ExecutionPlan with fewer
    /// partitions. This might happen, for example, if each new partition would
    /// be too small to be efficiently processed individually.
    ///
    /// The DataFusion optimizer attempts to use as many threads as possible by
    /// repartitioning its inputs to match the target number of threads
    /// available (`target_partitions`). Some data sources, such as the built in
    /// CSV and Parquet readers, implement this method as they are able to read
    /// from their input files in parallel, regardless of how the source data is
    /// split amongst files.
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    /// Begin execution of `partition`, returning a [`Stream`] of
    /// [`RecordBatch`]es.
    ///
    /// # Notes
    ///
    /// The `execute` method itself is not `async` but it returns an `async`
    /// [`futures::stream::Stream`]. This `Stream` should incrementally compute
    /// the output, `RecordBatch` by `RecordBatch` (in a streaming fashion).
    /// Most `ExecutionPlan`s should not do any work before the first
    /// `RecordBatch` is requested from the stream.
    ///
    /// [`RecordBatchStreamAdapter`] can be used to convert an `async`
    /// [`Stream`] into a [`SendableRecordBatchStream`].
    ///
    /// Using `async` `Streams` allows for network I/O during execution and
    /// takes advantage of Rust's built in support for `async` continuations and
    /// crate ecosystem.
    ///
    /// [`Stream`]: futures::stream::Stream
    /// [`StreamExt`]: futures::stream::StreamExt
    /// [`TryStreamExt`]: futures::stream::TryStreamExt
    /// [`RecordBatchStreamAdapter`]: crate::stream::RecordBatchStreamAdapter
    ///
    /// # Error handling
    ///
    /// Any error that occurs during execution is sent as an `Err` in the output
    /// stream.
    ///
    /// `ExecutionPlan` implementations in DataFusion cancel additional work
    /// immediately once an error occurs. The rationale is that if the overall
    /// query will return an error,  any additional work such as continued
    /// polling of inputs will be wasted as it will be thrown away.
    ///
    /// # Cancellation / Aborting Execution
    ///
    /// The [`Stream`] that is returned must ensure that any allocated resources
    /// are freed when the stream itself is dropped. This is particularly
    /// important for [`spawn`]ed tasks or threads. Unless care is taken to
    /// "abort" such tasks, they may continue to consume resources even after
    /// the plan is dropped, generating intermediate results that are never
    /// used.
    /// Thus, [`spawn`] is disallowed, and instead use [`SpawnedTask`].
    ///
    /// For more details see [`SpawnedTask`], [`JoinSet`] and [`RecordBatchReceiverStreamBuilder`]
    /// for structures to help ensure all background tasks are cancelled.
    ///
    /// [`spawn`]: tokio::task::spawn
    /// [`JoinSet`]: tokio::task::JoinSet
    /// [`SpawnedTask`]: datafusion_common_runtime::SpawnedTask
    /// [`RecordBatchReceiverStreamBuilder`]: crate::stream::RecordBatchReceiverStreamBuilder
    ///
    /// # Implementation Examples
    ///
    /// While `async` `Stream`s have a non trivial learning curve, the
    /// [`futures`] crate provides [`StreamExt`] and [`TryStreamExt`]
    /// which help simplify many common operations.
    ///
    /// Here are some common patterns:
    ///
    /// ## Return Precomputed `RecordBatch`
    ///
    /// We can return a precomputed `RecordBatch` as a `Stream`:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_schema::SchemaRef;
    /// # use datafusion_common::Result;
    /// # use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    /// # use datafusion_physical_plan::memory::MemoryStream;
    /// # use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    /// struct MyPlan {
    ///     batch: RecordBatch,
    /// }
    ///
    /// impl MyPlan {
    ///     fn execute(
    ///         &self,
    ///         partition: usize,
    ///         context: Arc<TaskContext>
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         // use functions from futures crate convert the batch into a stream
    ///         let fut = futures::future::ready(Ok(self.batch.clone()));
    ///         let stream = futures::stream::once(fut);
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(self.batch.schema(), stream)))
    ///     }
    /// }
    /// ```
    ///
    /// ## Lazily (async) Compute `RecordBatch`
    ///
    /// We can also lazily compute a `RecordBatch` when the returned `Stream` is polled
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_schema::SchemaRef;
    /// # use datafusion_common::Result;
    /// # use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    /// # use datafusion_physical_plan::memory::MemoryStream;
    /// # use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    /// struct MyPlan {
    ///     schema: SchemaRef,
    /// }
    ///
    /// /// Returns a single batch when the returned stream is polled
    /// async fn get_batch() -> Result<RecordBatch> {
    ///     todo!()
    /// }
    ///
    /// impl MyPlan {
    ///     fn execute(
    ///         &self,
    ///         partition: usize,
    ///         context: Arc<TaskContext>
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         let fut = get_batch();
    ///         let stream = futures::stream::once(fut);
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
    ///     }
    /// }
    /// ```
    ///
    /// ## Lazily (async) create a Stream
    ///
    /// If you need to create the return `Stream` using an `async` function,
    /// you can do so by flattening the result:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_schema::SchemaRef;
    /// # use futures::TryStreamExt;
    /// # use datafusion_common::Result;
    /// # use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    /// # use datafusion_physical_plan::memory::MemoryStream;
    /// # use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    /// struct MyPlan {
    ///     schema: SchemaRef,
    /// }
    ///
    /// /// async function that returns a stream
    /// async fn get_batch_stream() -> Result<SendableRecordBatchStream> {
    ///     todo!()
    /// }
    ///
    /// impl MyPlan {
    ///     fn execute(
    ///         &self,
    ///         partition: usize,
    ///         context: Arc<TaskContext>
    ///     ) -> Result<SendableRecordBatchStream> {
    ///         // A future that yields a stream
    ///         let fut = get_batch_stream();
    ///         // Use TryStreamExt::try_flatten to flatten the stream of streams
    ///         let stream = futures::stream::once(fut).try_flatten();
    ///         Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
    ///     }
    /// }
    /// ```
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;

    /// Return a snapshot of the set of [`Metric`]s for this
    /// [`ExecutionPlan`]. If no `Metric`s are available, return None.
    ///
    /// While the values of the metrics in the returned
    /// [`MetricsSet`]s may change as execution progresses, the
    /// specific metrics will not.
    ///
    /// Once `self.execute()` has returned (technically the future is
    /// resolved) for all available partitions, the set of metrics
    /// should be complete. If this function is called prior to
    /// `execute()` new metrics may appear in subsequent calls.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Returns statistics for this `ExecutionPlan` node. If statistics are not
    /// available, should return [`Statistics::new_unknown`] (the default), not
    /// an error.
    ///
    /// For TableScan executors, which supports filter pushdown, special attention
    /// needs to be paid to whether the stats returned by this method are exact or not
    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    /// Returns `true` if a limit can be safely pushed down through this
    /// `ExecutionPlan` node.
    ///
    /// If this method returns `true`, and the query plan contains a limit at
    /// the output of this node, DataFusion will push the limit to the input
    /// of this node.
    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    /// Returns a fetching variant of this `ExecutionPlan` node, if it supports
    /// fetch limits. Returns `None` otherwise.
    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    /// Gets the fetch count for the operator, `None` means there is no fetch.
    fn fetch(&self) -> Option<usize> {
        None
    }

    /// Gets the effect on cardinality, if known
    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Unknown
    }

    /// whether it is a physical scan or not
    fn support_dynamic_filter(&self) -> bool {
        false
    }

    /// return a new execution plan with dynamic filter
    fn with_dynamic_filter(
        &self,
        _dynamic_filters: Option<Arc<DynamicFilterInfo>>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}

/// Extension trait provides an easy API to fetch various properties of
/// [`ExecutionPlan`] objects based on [`ExecutionPlan::properties`].
pub trait ExecutionPlanProperties {
    /// Specifies how the output of this `ExecutionPlan` is split into
    /// partitions.
    fn output_partitioning(&self) -> &Partitioning;

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns [`ExecutionMode::PipelineBreaking`] to indicate this.
    fn execution_mode(&self) -> ExecutionMode;

    /// If the output of this `ExecutionPlan` within each partition is sorted,
    /// returns `Some(keys)` describing the ordering. A `None` return value
    /// indicates no assumptions should be made on the output ordering.
    ///
    /// For example, `SortExec` (obviously) produces sorted output as does
    /// `SortPreservingMergeStream`. Less obviously, `Projection` produces sorted
    /// output if its input is sorted as it does not reorder the input rows.
    fn output_ordering(&self) -> Option<&LexOrdering>;

    /// Get the [`EquivalenceProperties`] within the plan.
    ///
    /// Equivalence properties tell DataFusion what columns are known to be
    /// equal, during various optimization passes. By default, this returns "no
    /// known equivalences" which is always correct, but may cause DataFusion to
    /// unnecessarily resort data.
    ///
    /// If this ExecutionPlan makes no changes to the schema of the rows flowing
    /// through it or how columns within each row relate to each other, it
    /// should return the equivalence properties of its input. For
    /// example, since `FilterExec` may remove rows from its input, but does not
    /// otherwise modify them, it preserves its input equivalence properties.
    /// However, since `ProjectionExec` may calculate derived expressions, it
    /// needs special handling.
    ///
    /// See also [`ExecutionPlan::maintains_input_order`] and [`Self::output_ordering`]
    /// for related concepts.
    fn equivalence_properties(&self) -> &EquivalenceProperties;
}

impl ExecutionPlanProperties for Arc<dyn ExecutionPlan> {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties().output_partitioning()
    }

    fn execution_mode(&self) -> ExecutionMode {
        self.properties().execution_mode()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        self.properties().output_ordering()
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties().equivalence_properties()
    }
}

impl ExecutionPlanProperties for &dyn ExecutionPlan {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties().output_partitioning()
    }

    fn execution_mode(&self) -> ExecutionMode {
        self.properties().execution_mode()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        self.properties().output_ordering()
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties().equivalence_properties()
    }
}

/// Describes the execution mode of the result of calling
/// [`ExecutionPlan::execute`] with respect to its size and behavior.
///
/// The mode of the execution plan is determined by the mode of its input
/// execution plans and the details of the operator itself. For example, a
/// `FilterExec` operator will have the same execution mode as its input, but a
/// `SortExec` operator may have a different execution mode than its input,
/// depending on how the input stream is sorted.
///
/// There are three possible execution modes: `Bounded`, `Unbounded` and
/// `PipelineBreaking`.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ExecutionMode {
    /// The stream is bounded / finite.
    ///
    /// In this case the stream will eventually return `None` to indicate that
    /// there are no more records to process.
    Bounded,
    /// The stream is unbounded / infinite.
    ///
    /// In this case, the stream will never be done (never return `None`),
    /// except in case of error.
    ///
    /// This mode is often used in "Steaming" use cases where data is
    /// incrementally processed as it arrives.
    ///
    /// Note that even though the operator generates an unbounded stream of
    /// results, it can execute with bounded memory and incrementally produces
    /// output.
    Unbounded,
    /// Some of the operator's input stream(s) are unbounded, but the operator
    /// cannot generate streaming results from these streaming inputs.
    ///
    /// In this case, the execution mode will be pipeline breaking, e.g. the
    /// operator requires unbounded memory to generate results. This
    /// information is used by the planner when performing sanity checks
    /// on plans processings unbounded data sources.
    PipelineBreaking,
}

impl ExecutionMode {
    /// Check whether the execution mode is unbounded or not.
    pub fn is_unbounded(&self) -> bool {
        matches!(self, ExecutionMode::Unbounded)
    }

    /// Check whether the execution is pipeline friendly. If so, operator can
    /// execute safely.
    pub fn pipeline_friendly(&self) -> bool {
        matches!(self, ExecutionMode::Bounded | ExecutionMode::Unbounded)
    }
}

/// Conservatively "combines" execution modes of a given collection of operators.
pub(crate) fn execution_mode_from_children<'a>(
    children: impl IntoIterator<Item = &'a Arc<dyn ExecutionPlan>>,
) -> ExecutionMode {
    let mut result = ExecutionMode::Bounded;
    for mode in children.into_iter().map(|child| child.execution_mode()) {
        match (mode, result) {
            (ExecutionMode::PipelineBreaking, _)
            | (_, ExecutionMode::PipelineBreaking) => {
                // If any of the modes is `PipelineBreaking`, so is the result:
                return ExecutionMode::PipelineBreaking;
            }
            (ExecutionMode::Unbounded, _) | (_, ExecutionMode::Unbounded) => {
                // Unbounded mode eats up bounded mode:
                result = ExecutionMode::Unbounded;
            }
            (ExecutionMode::Bounded, ExecutionMode::Bounded) => {
                // When both modes are bounded, so is the result:
                result = ExecutionMode::Bounded;
            }
        }
    }
    result
}

/// Stores certain, often expensive to compute, plan properties used in query
/// optimization.
///
/// These properties are stored a single structure to permit this information to
/// be computed once and then those cached results used multiple times without
/// recomputation (aka a cache)
#[derive(Debug, Clone)]
pub struct PlanProperties {
    /// See [ExecutionPlanProperties::equivalence_properties]
    pub eq_properties: EquivalenceProperties,
    /// See [ExecutionPlanProperties::output_partitioning]
    pub partitioning: Partitioning,
    /// See [ExecutionPlanProperties::execution_mode]
    pub execution_mode: ExecutionMode,
    /// See [ExecutionPlanProperties::output_ordering]
    output_ordering: Option<LexOrdering>,
}

impl PlanProperties {
    /// Construct a new `PlanPropertiesCache` from the
    pub fn new(
        eq_properties: EquivalenceProperties,
        partitioning: Partitioning,
        execution_mode: ExecutionMode,
    ) -> Self {
        // Output ordering can be derived from `eq_properties`.
        let output_ordering = eq_properties.output_ordering();
        Self {
            eq_properties,
            partitioning,
            execution_mode,
            output_ordering,
        }
    }

    /// Overwrite output partitioning with its new value.
    pub fn with_partitioning(mut self, partitioning: Partitioning) -> Self {
        self.partitioning = partitioning;
        self
    }

    /// Overwrite the execution Mode with its new value.
    pub fn with_execution_mode(mut self, execution_mode: ExecutionMode) -> Self {
        self.execution_mode = execution_mode;
        self
    }

    /// Overwrite equivalence properties with its new value.
    pub fn with_eq_properties(mut self, eq_properties: EquivalenceProperties) -> Self {
        // Changing equivalence properties also changes output ordering, so
        // make sure to overwrite it:
        self.output_ordering = eq_properties.output_ordering();
        self.eq_properties = eq_properties;
        self
    }

    pub fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.eq_properties
    }

    pub fn output_partitioning(&self) -> &Partitioning {
        &self.partitioning
    }

    pub fn output_ordering(&self) -> Option<&LexOrdering> {
        self.output_ordering.as_ref()
    }

    pub fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode
    }

    /// Get schema of the node.
    fn schema(&self) -> &SchemaRef {
        self.eq_properties.schema()
    }
}

/// Indicate whether a data exchange is needed for the input of `plan`, which will be very helpful
/// especially for the distributed engine to judge whether need to deal with shuffling.
/// Currently there are 3 kinds of execution plan which needs data exchange
///     1. RepartitionExec for changing the partition number between two `ExecutionPlan`s
///     2. CoalescePartitionsExec for collapsing all of the partitions into one without ordering guarantee
///     3. SortPreservingMergeExec for collapsing all of the sorted partitions into one with ordering guarantee
pub fn need_data_exchange(plan: Arc<dyn ExecutionPlan>) -> bool {
    if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        !matches!(
            repartition.properties().output_partitioning(),
            Partitioning::RoundRobinBatch(_)
        )
    } else if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>()
    {
        coalesce.input().output_partitioning().partition_count() > 1
    } else if let Some(sort_preserving_merge) =
        plan.as_any().downcast_ref::<SortPreservingMergeExec>()
    {
        sort_preserving_merge
            .input()
            .output_partitioning()
            .partition_count()
            > 1
    } else {
        false
    }
}

/// Returns a copy of this plan if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `ExecutionPlan::children()`.
pub fn with_new_children_if_necessary(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let old_children = plan.children();
    if children.len() != old_children.len() {
        internal_err!("Wrong number of children")
    } else if children.is_empty()
        || children
            .iter()
            .zip(old_children.iter())
            .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
    {
        plan.with_new_children(children)
    } else {
        Ok(plan)
    }
}

/// Return a [wrapper](DisplayableExecutionPlan) around an
/// [`ExecutionPlan`] which can be displayed in various easier to
/// understand ways.
pub fn displayable(plan: &dyn ExecutionPlan) -> DisplayableExecutionPlan<'_> {
    DisplayableExecutionPlan::new(plan)
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let stream = execute_stream(plan, context)?;
    crate::common::collect(stream).await
}

/// Execute the [ExecutionPlan] and return a single stream of `RecordBatch`es.
///
/// See [collect] to buffer the `RecordBatch`es in memory.
///
/// # Aborting Execution
///
/// Dropping the stream will abort the execution of the query, and free up
/// any allocated resources
pub fn execute_stream(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    match plan.output_partitioning().partition_count() {
        0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
        1 => plan.execute(0, context),
        2.. => {
            // merge into a single partition
            let plan = CoalescePartitionsExec::new(Arc::clone(&plan));
            // CoalescePartitionsExec must produce a single partition
            assert_eq!(1, plan.properties().output_partitioning().partition_count());
            plan.execute(0, context)
        }
    }
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<Vec<RecordBatch>>> {
    let streams = execute_stream_partitioned(plan, context)?;

    let mut join_set = JoinSet::new();
    // Execute the plan and collect the results into batches.
    streams.into_iter().enumerate().for_each(|(idx, stream)| {
        join_set.spawn(async move {
            let result: Result<Vec<RecordBatch>> = stream.try_collect().await;
            (idx, result)
        });
    });

    let mut batches = vec![];
    // Note that currently this doesn't identify the thread that panicked
    //
    // TODO: Replace with [join_next_with_id](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html#method.join_next_with_id
    // once it is stable
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((idx, res)) => batches.push((idx, res?)),
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    batches.sort_by_key(|(idx, _)| *idx);
    let batches = batches.into_iter().map(|(_, batch)| batch).collect();

    Ok(batches)
}

/// Execute the [ExecutionPlan] and return a vec with one stream per output
/// partition
///
/// # Aborting Execution
///
/// Dropping the stream will abort the execution of the query, and free up
/// any allocated resources
pub fn execute_stream_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<SendableRecordBatchStream>> {
    let num_partitions = plan.output_partitioning().partition_count();
    let mut streams = Vec::with_capacity(num_partitions);
    for i in 0..num_partitions {
        streams.push(plan.execute(i, Arc::clone(&context))?);
    }
    Ok(streams)
}

/// Executes an input stream and ensures that the resulting stream adheres to
/// the `not null` constraints specified in the `sink_schema`.
///
/// # Arguments
///
/// * `input` - An execution plan
/// * `sink_schema` - The schema to be applied to the output stream
/// * `partition` - The partition index to be executed
/// * `context` - The task context
///
/// # Returns
///
/// * `Result<SendableRecordBatchStream>` - A stream of `RecordBatch`es if successful
///
/// This function first executes the given input plan for the specified partition
/// and context. It then checks if there are any columns in the input that might
/// violate the `not null` constraints specified in the `sink_schema`. If there are
/// such columns, it wraps the resulting stream to enforce the `not null` constraints
/// by invoking the `check_not_null_contraits` function on each batch of the stream.
pub fn execute_input_stream(
    input: Arc<dyn ExecutionPlan>,
    sink_schema: SchemaRef,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let input_stream = input.execute(partition, context)?;

    debug_assert_eq!(sink_schema.fields().len(), input.schema().fields().len());

    // Find input columns that may violate the not null constraint.
    let risky_columns: Vec<_> = sink_schema
        .fields()
        .iter()
        .zip(input.schema().fields().iter())
        .enumerate()
        .filter_map(|(idx, (sink_field, input_field))| {
            (!sink_field.is_nullable() && input_field.is_nullable()).then_some(idx)
        })
        .collect();

    if risky_columns.is_empty() {
        Ok(input_stream)
    } else {
        // Check not null constraint on the input stream
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            sink_schema,
            input_stream
                .map(move |batch| check_not_null_constraints(batch?, &risky_columns)),
        )))
    }
}

/// Checks a `RecordBatch` for `not null` constraints on specified columns.
///
/// # Arguments
///
/// * `batch` - The `RecordBatch` to be checked
/// * `column_indices` - A vector of column indices that should be checked for
///   `not null` constraints.
///
/// # Returns
///
/// * `Result<RecordBatch>` - The original `RecordBatch` if all constraints are met
///
/// This function iterates over the specified column indices and ensures that none
/// of the columns contain null values. If any column contains null values, an error
/// is returned.
pub fn check_not_null_constraints(
    batch: RecordBatch,
    column_indices: &Vec<usize>,
) -> Result<RecordBatch> {
    for &index in column_indices {
        if batch.num_columns() <= index {
            return exec_err!(
                "Invalid batch column count {} expected > {}",
                batch.num_columns(),
                index
            );
        }

        if batch
            .column(index)
            .logical_nulls()
            .map(|nulls| nulls.null_count())
            .unwrap_or_default()
            > 0
        {
            return exec_err!(
                "Invalid batch column at '{}' has null but schema specifies non-nullable",
                index
            );
        }
    }

    Ok(batch)
}

/// Utility function yielding a string representation of the given [`ExecutionPlan`].
pub fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let formatted = displayable(plan.as_ref()).indent(true).to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    actual.iter().map(|elem| elem.to_string()).collect()
}

/// Indicates the effect an execution plan operator will have on the cardinality
/// of its input stream
pub enum CardinalityEffect {
    /// Unknown effect. This is the default
    Unknown,
    /// The operator is guaranteed to produce exactly one row for
    /// each input row
    Equal,
    /// The operator may produce fewer output rows than it receives input rows
    LowerEqual,
    /// The operator may produce more output rows than it receives input rows
    GreaterEqual,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{DictionaryArray, Int32Array, NullArray, RunArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::any::Any;
    use std::sync::Arc;

    use datafusion_common::{Result, Statistics};
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};

    use crate::{DisplayAs, DisplayFormatType, ExecutionPlan};

    #[derive(Debug)]
    pub struct EmptyExec;

    impl EmptyExec {
        pub fn new(_schema: SchemaRef) -> Self {
            Self
        }
    }

    impl DisplayAs for EmptyExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for EmptyExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }

        fn statistics(&self) -> Result<Statistics> {
            unimplemented!()
        }
    }

    #[derive(Debug)]
    pub struct RenamedEmptyExec;

    impl RenamedEmptyExec {
        pub fn new(_schema: SchemaRef) -> Self {
            Self
        }
    }

    impl DisplayAs for RenamedEmptyExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for RenamedEmptyExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn static_name() -> &'static str
        where
            Self: Sized,
        {
            "MyRenamedEmptyExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }

        fn statistics(&self) -> Result<Statistics> {
            unimplemented!()
        }
    }

    #[test]
    fn test_execution_plan_name() {
        let schema1 = Arc::new(Schema::empty());
        let default_name_exec = EmptyExec::new(schema1);
        assert_eq!(default_name_exec.name(), "EmptyExec");

        let schema2 = Arc::new(Schema::empty());
        let renamed_exec = RenamedEmptyExec::new(schema2);
        assert_eq!(renamed_exec.name(), "MyRenamedEmptyExec");
        assert_eq!(RenamedEmptyExec::static_name(), "MyRenamedEmptyExec");
    }

    /// A compilation test to ensure that the `ExecutionPlan::name()` method can
    /// be called from a trait object.
    /// Related ticket: https://github.com/apache/datafusion/pull/11047
    #[allow(dead_code)]
    fn use_execution_plan_as_trait_object(plan: &dyn ExecutionPlan) {
        let _ = plan.name();
    }

    #[test]
    fn test_check_not_null_constraints_accept_non_null() -> Result<()> {
        check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
                vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
            )?,
            &vec![0],
        )?;
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_reject_null() -> Result<()> {
        let result = check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
                vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
            )?,
            &vec![0],
        );
        assert!(result.is_err());
        assert_starts_with(
            result.err().unwrap().message().as_ref(),
            "Invalid batch column at '0' has null but schema specifies non-nullable",
        );
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_with_run_end_array() -> Result<()> {
        // some null value inside REE array
        let run_ends = Int32Array::from(vec![1, 2, 3, 4]);
        let values = Int32Array::from(vec![Some(0), None, Some(1), None]);
        let run_end_array = RunArray::try_new(&run_ends, &values)?;
        let result = check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "a",
                    run_end_array.data_type().to_owned(),
                    true,
                )])),
                vec![Arc::new(run_end_array)],
            )?,
            &vec![0],
        );
        assert!(result.is_err());
        assert_starts_with(
            result.err().unwrap().message().as_ref(),
            "Invalid batch column at '0' has null but schema specifies non-nullable",
        );
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_with_dictionary_array_with_null() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)]));
        let keys = Int32Array::from(vec![0, 1, 2, 3]);
        let dictionary = DictionaryArray::new(keys, values);
        let result = check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "a",
                    dictionary.data_type().to_owned(),
                    true,
                )])),
                vec![Arc::new(dictionary)],
            )?,
            &vec![0],
        );
        assert!(result.is_err());
        assert_starts_with(
            result.err().unwrap().message().as_ref(),
            "Invalid batch column at '0' has null but schema specifies non-nullable",
        );
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_with_dictionary_masking_null() -> Result<()> {
        // some null value marked out by dictionary array
        let values = Arc::new(Int32Array::from(vec![
            Some(1),
            None, // this null value is masked by dictionary keys
            Some(3),
            Some(4),
        ]));
        let keys = Int32Array::from(vec![0, /*1,*/ 2, 3]);
        let dictionary = DictionaryArray::new(keys, values);
        check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "a",
                    dictionary.data_type().to_owned(),
                    true,
                )])),
                vec![Arc::new(dictionary)],
            )?,
            &vec![0],
        )?;
        Ok(())
    }

    #[test]
    fn test_check_not_null_constraints_on_null_type() -> Result<()> {
        // null value of Null type
        let result = check_not_null_constraints(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Null, true)])),
                vec![Arc::new(NullArray::new(3))],
            )?,
            &vec![0],
        );
        assert!(result.is_err());
        assert_starts_with(
            result.err().unwrap().message().as_ref(),
            "Invalid batch column at '0' has null but schema specifies non-nullable",
        );
        Ok(())
    }

    fn assert_starts_with(actual: impl AsRef<str>, expected_prefix: impl AsRef<str>) {
        let actual = actual.as_ref();
        let expected_prefix = expected_prefix.as_ref();
        assert!(
            actual.starts_with(expected_prefix),
            "Expected '{}' to start with '{}'",
            actual,
            expected_prefix
        );
    }
}
