use async_std::stream;
use async_std::stream::StreamExt;
use async_trait::async_trait;

use crate::block::ExecutionMetadataRef;
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};

pub struct StreamSource<Out> {
    inner: Box<dyn stream::Stream<Item = Out> + Unpin + Send>,
}

impl<Out> StreamSource<Out> {
    pub fn new<S>(inner: S) -> Self
    where
        S: stream::Stream<Item = Out> + Unpin + Send + 'static,
    {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl<Out> Source<Out> for StreamSource<Out> where Out: Send + Unpin + 'static {}

#[async_trait]
impl<Out> Operator<Out> for StreamSource<Out>
where
    Out: Send + Unpin + 'static,
{
    fn init(&mut self, _metadata: ExecutionMetadataRef) {}

    async fn next(&mut self) -> StreamElement<Out> {
        match self.inner.next().await {
            Some(t) => StreamElement::Item(t),
            None => StreamElement::End,
        }
    }

    fn to_string(&self) -> String {
        format!("StreamSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out> Clone for StreamSource<Out>
where
    Out: Send + Unpin + 'static,
{
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        Self {
            inner: Box::new(stream::empty()),
        }
    }
}
