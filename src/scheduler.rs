use std::collections::HashMap;

use async_std::channel::Sender;
use async_std::sync::{Arc, Mutex};
use async_std::task::JoinHandle;

use crate::block::InnerBlock;
use crate::config::{EnvironmentConfig, ExecutionRuntime, LocalRuntimeConfig};
use crate::network::{Coord, NetworkTopology};
use crate::operator::Operator;
use crate::stream::BlockId;
use crate::worker::spawn_worker;

pub type ReplicaId = usize;

pub struct ExecutionMetadata {
    pub coord: Coord,
    pub network: Arc<Mutex<NetworkTopology>>,
}

pub struct StartHandle {
    pub starter: Sender<ExecutionMetadata>,
    pub join_handle: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy)]
pub struct SchedulerBlockInfo {
    num_replicas: usize,
}

pub struct Scheduler {
    pub config: EnvironmentConfig,
    pub next_blocks: HashMap<BlockId, Vec<BlockId>>,
    pub block_info: HashMap<BlockId, SchedulerBlockInfo>,
    pub start_handles: HashMap<BlockId, Vec<StartHandle>>,
    pub network: NetworkTopology,
}

impl Scheduler {
    pub fn new(config: EnvironmentConfig) -> Self {
        Self {
            config,
            next_blocks: Default::default(),
            block_info: Default::default(),
            start_handles: Default::default(),
            network: NetworkTopology::new(),
        }
    }

    pub fn add_block<In, Out, OperatorChain>(&mut self, block: InnerBlock<In, Out, OperatorChain>)
    where
        In: Clone + Send + 'static,
        Out: Clone + Send + 'static,
        OperatorChain: Operator<Out> + Send + 'static,
    {
        let block_id = block.id;
        let info = self.local_block_info(&block);
        info!("Adding new block id={}: {:?}", block_id, info);
        self.block_info.insert(block_id, info);
        let mut blocks = vec![block];
        blocks.reserve(info.num_replicas);
        for _ in 0..info.num_replicas - 1 {
            // avoid an extra clone, this will make the metadata unique
            blocks.push(blocks[0].clone());
        }
        for (replica_id, mut block) in blocks.into_iter().enumerate() {
            let coord = Coord::new(block_id, replica_id);
            // register this block in the network
            self.network.register_local::<In>(coord);
            // initialize the block with its metadata ref (it will be set at start)
            block.operators.block_init(block.execution_metadata.clone());
            // spawn the actual worker
            let start_handle = spawn_worker(block);
            self.start_handles
                .entry(block_id)
                .or_default()
                .push(start_handle);
        }
    }

    pub fn connect_blocks(&mut self, from: BlockId, to: BlockId) {
        info!("Connecting blocks: {} -> {}", from, to);
        if !self.start_handles.contains_key(&from) {
            panic!("Connecting from an unknown block: {}", from);
        }
        self.next_blocks.entry(from).or_default().push(to);
    }

    pub async fn start(self) -> Vec<JoinHandle<()>> {
        match self.config.runtime {
            ExecutionRuntime::Local(local) => self.start_local(local).await,
        }
    }

    async fn start_local(mut self, config: LocalRuntimeConfig) -> Vec<JoinHandle<()>> {
        info!("Starting local environment: {:?}", config);
        self.setup_topology();
        self.log_topology();
        self.network.log_topology();

        let mut join = Vec::new();
        let network = Arc::new(Mutex::new(self.network));
        // start the execution
        for (block_id, handles) in self.start_handles.drain() {
            for (replica_id, handle) in handles.into_iter().enumerate() {
                let metadata = ExecutionMetadata {
                    coord: Coord::new(block_id, replica_id),
                    network: network.clone(),
                };
                handle.starter.send(metadata).await.unwrap();
                join.push(handle.join_handle);
            }
        }
        join
    }

    fn setup_topology(&mut self) {
        for (from_block_id, next) in self.next_blocks.iter() {
            let from_info = self.block_info[from_block_id];
            for to_block_id in next.iter() {
                let to_info = self.block_info[to_block_id];
                for from_replica_id in 0..from_info.num_replicas {
                    let from_coord = Coord::new(*from_block_id, from_replica_id);
                    for to_replica_id in 0..to_info.num_replicas {
                        let to_coord = Coord::new(*to_block_id, to_replica_id);
                        self.network.connect(from_coord, to_coord);
                    }
                }
            }
        }
    }

    fn log_topology(&self) {
        debug!("Job graph:");
        for (id, next) in self.next_blocks.iter() {
            debug!("  {}: {:?}", id, next);
        }
    }

    fn local_block_info<In, Out, OperatorChain>(
        &self,
        _block: &InnerBlock<In, Out, OperatorChain>,
    ) -> SchedulerBlockInfo
    where
        In: Clone + Send + 'static,
        Out: Clone + Send + 'static,
        OperatorChain: Operator<Out>,
    {
        match self.config.runtime {
            ExecutionRuntime::Local(local) => SchedulerBlockInfo {
                num_replicas: local.num_cores,
            },
        }
    }
}
