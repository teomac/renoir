use std::marker::PhantomData;

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::block::ExecutionMetadataRef;
use crate::operator::{Operator, StreamElement};
use crate::stream::Stream;

pub struct Map<Out, NewOut, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    f: Arc<dyn Fn(Out) -> NewOut + Send + Sync>,
    _out_type: PhantomData<Out>,
    _new_out_type: PhantomData<NewOut>,
}

#[async_trait]
impl<Out, NewOut, PreviousOperators> Operator<NewOut> for Map<Out, NewOut, PreviousOperators>
where
    Out: Send,
    NewOut: Send,
    PreviousOperators: Operator<Out> + Send,
{
    fn init(&mut self, metadata: ExecutionMetadataRef) {
        self.prev.init(metadata);
    }

    async fn next(&mut self) -> StreamElement<NewOut> {
        match self.prev.next().await {
            StreamElement::Item(t) => StreamElement::Item((self.f)(t)),
            StreamElement::Timestamped(t, ts) => StreamElement::Timestamped((self.f)(t), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => StreamElement::End,
        }
    }

    fn to_string(&self) -> String {
        format!(
            "Map<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Out, NewOut, PreviousOperators> Clone for Map<Out, NewOut, PreviousOperators>
where
    Out: Send,
    NewOut: Send,
    PreviousOperators: Operator<Out> + Send,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            f: self.f.clone(),
            _out_type: Default::default(),
            _new_out_type: Default::default(),
        }
    }
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn map<NewOut, F>(self, f: F) -> Stream<In, NewOut, Map<Out, NewOut, OperatorChain>>
    where
        In: Send + 'static,
        Out: Send + 'static,
        NewOut: Send + 'static,
        F: Fn(Out) -> NewOut + Send + Sync + 'static,
    {
        self.add_operator(|prev| Map {
            prev,
            f: Arc::new(f),
            _out_type: Default::default(),
            _new_out_type: Default::default(),
        })
    }
}
