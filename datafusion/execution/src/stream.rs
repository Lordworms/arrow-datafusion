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

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch, row::Rows};
use datafusion_common::{DataFusionError, Result};
use futures::Stream;
use pin_project_lite::pin_project;
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};
/// Trait for types that stream [RecordBatch]
///
/// See [`SendableRecordBatchStream`] for more details.
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

/// Trait for a [`Stream`] of [`RecordBatch`]es that can be passed between threads
///
/// This trait is used to retrieve the results of DataFusion execution plan nodes.
///
/// The trait is a specialized Rust Async [`Stream`] that also knows the schema
/// of the data it will return (even if the stream has no data). Every
/// `RecordBatch` returned by the stream should have the same schema as returned
/// by [`schema`](`RecordBatchStream::schema`).
///
/// # Error Handling
///
/// Once a stream returns an error, it should not be polled again (the caller
/// should stop calling `next`) and handle the error.
///
/// However, returning `Ready(None)` (end of stream) is likely the safest
/// behavior after an error. Like [`Stream`]s, `RecordBatchStream`s should not
/// be polled after end of stream or returning an error. However, also like
/// [`Stream`]s there is no mechanism to prevent callers polling  so returning
/// `Ready(None)` is recommended.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

pub enum RowOrColumn {
    Row(Rows),
    Column(RecordBatch),
}

/// Contains a Rows or a Recordbatch
pub type RowOrColumnStream = Pin<Box<dyn Stream<Item = Result<RowOrColumn>> + Send>>;

pin_project! {
    pub struct RowOrColumnStreamAdapter<S> {
        #[pin]
        stream: S,
    }
}

impl<S> RowOrColumnStreamAdapter<S> {
    pub fn new(stream: S) -> Self {
        Self { stream }
    }
}

impl<S> fmt::Debug for RowOrColumnStreamAdapter<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowOrColumnStreamAdapter").finish()
    }
}

impl<S> Stream for RowOrColumnStreamAdapter<S>
where
    S: Stream<Item = Result<RowOrColumn>> + Unpin,
{
    type Item = Result<RowOrColumn, ()>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S> From<RowOrColumnStreamAdapter<S>> for RowOrColumnStream
where
    S: Stream<Item = Result<RowOrColumn, DataFusionError>> + Send + 'static,
{
    fn from(adapter: RowOrColumnStreamAdapter<S>) -> Self {
        Box::pin(adapter)
    }
}
