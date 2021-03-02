use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::block::{InnerBlock, NextStrategy};
use crate::operator::StartBlock;
use crate::operator::{broadcast, SenderList};
use crate::operator::{KeyBy, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyedStream, Stream};

pub type Keyer<Key, Out> = Arc<dyn Fn(&Out) -> Key + Send + Sync>;

pub struct GroupByEndBlock<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    metadata: Option<ExecutionMetadata>,
    keyer: Keyer<Key, Out>,
    senders: SenderList<Out>,
}

impl<Key, Out, OperatorChain> GroupByEndBlock<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub fn new(prev: OperatorChain, keyer: Keyer<Key, Out>) -> Self {
        Self {
            prev,
            metadata: None,
            keyer,
            senders: Default::default(),
        }
    }
}

async fn send<Key, Out>(
    senders: &SenderList<Out>,
    message: StreamElement<Out>,
    keyer: &Keyer<Key, Out>,
) where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
{
    let mut s = DefaultHasher::new();
    match &message {
        StreamElement::Item(item) | StreamElement::Timestamped(item, _) => keyer(item).hash(&mut s),
        _ => unreachable!("GroupBy can directly send only items"),
    }
    let hash = s.finish() as usize;
    for senders in senders.iter() {
        let sender = &senders[hash % senders.len()];
        let out_buf = vec![message.clone()];
        if let Err(e) = sender.send(out_buf).await {
            error!("Failed to send message to {:?}: {:?}", sender, e);
        }
    }
}

#[async_trait]
impl<Key, Out, OperatorChain> Operator<()> for GroupByEndBlock<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata.clone()).await;

        let network = metadata.network.lock().await;
        let senders = network.get_senders(metadata.coord);
        drop(network);
        let mut by_block_id: HashMap<_, Vec<_>> = HashMap::new();
        for (coord, sender) in senders {
            by_block_id.entry(coord.block_id).or_default().push(sender);
        }
        for (_block_id, mut senders) in by_block_id {
            senders.sort_by_key(|s| s.coord.replica_id);
            self.senders.push(senders);
        }
        info!(
            "GroupByEndBlock of {} has these senders: {:?}",
            metadata.coord, self.senders
        );
        self.metadata = Some(metadata);
    }

    async fn next(&mut self) -> StreamElement<()> {
        let message = self.prev.next().await;
        let to_return = message.take();
        match message {
            StreamElement::Watermark(_) | StreamElement::End => {
                let out_buf = vec![message];
                broadcast(&self.senders, out_buf).await
            }
            _ => send(&self.senders, message, &self.keyer).await,
        };
        if matches!(to_return, StreamElement::End) {
            let metadata = self.metadata.as_ref().unwrap();
            info!("GroupByEndBlock at {} received End", metadata.coord);
        }
        to_return
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> GroupBy<{}>",
            self.prev.to_string(),
            std::any::type_name::<Key>()
        )
    }
}

impl<Key, Out, OperatorChain> Clone for GroupByEndBlock<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    fn clone(&self) -> Self {
        if self.metadata.is_some() {
            panic!("Cannot clone once initialized");
        }
        Self {
            prev: self.prev.clone(),
            metadata: None,
            keyer: self.keyer.clone(),
            senders: Default::default(),
        }
    }
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn group_by<Key, Keyer>(
        mut self,
        keyer: Keyer,
    ) -> KeyedStream<Out, Key, Out, KeyBy<Key, Out, StartBlock<Out>>>
    where
        Key: Clone + Send + Hash + Eq + 'static,
        Keyer: Fn(&Out) -> Key + Send + Sync + 'static,
    {
        let keyer = Arc::new(keyer);
        self.block.next_strategy = NextStrategy::GroupBy;
        let old_stream = self.add_operator(|prev| GroupByEndBlock::new(prev, keyer.clone()));
        let mut env = old_stream.env.borrow_mut();
        let new_id = env.new_block();
        let scheduler = env.scheduler_mut();
        scheduler.add_block(old_stream.block);
        scheduler.connect_blocks(old_stream.block_id, new_id);
        drop(env);
        KeyedStream(Stream {
            block_id: new_id,
            block: InnerBlock::new(new_id, KeyBy::new(StartBlock::new(), keyer)),
            env: old_stream.env,
        })
    }
}

impl<Key, Out, OperatorChain> Debug for GroupByEndBlock<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupByEndBlock")
            .field("prev", &self.prev)
            .field("metadata", &self.metadata)
            .field("keyer", &std::any::type_name::<Keyer<Key, Out>>())
            .field("senders", &self.senders)
            .finish()
    }
}