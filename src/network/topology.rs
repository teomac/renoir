use std::any::TypeId;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::marker::PhantomData;
use std::sync::Arc;
#[cfg(not(feature = "tokio"))]
use std::thread::JoinHandle;

#[cfg(feature = "tokio")]
use futures::StreamExt;
use indexmap::IndexSet;
use typemap_rev::{TypeMap, TypeMapKey};

use crate::channel::Sender;
use crate::config::RuntimeConfig;
use crate::network::demultiplexer::DemuxHandle;
use crate::network::multiplexer::MultiplexingSender;
use crate::network::{
    local_channel, BlockCoord, Coord, DemuxCoord, NetworkReceiver, NetworkSender, ReceiverEndpoint,
};
use crate::operator::ExchangeData;
use crate::scheduler::{BlockId, HostId};

use super::NetworkMessage;

/// This struct is used to index inside the `typemap` with the `NetworkReceiver`s.
struct ReceiverKey<In: ExchangeData>(PhantomData<In>);

impl<In: ExchangeData> TypeMapKey for ReceiverKey<In> {
    type Value = HashMap<ReceiverEndpoint, NetworkReceiver<In>, crate::block::CoordHasherBuilder>;
}

/// This struct is used to index inside the `typemap` with the `NetworkSender`s.
struct SenderKey<In: ExchangeData>(PhantomData<In>);

impl<In: ExchangeData> TypeMapKey for SenderKey<In> {
    type Value = HashMap<ReceiverEndpoint, NetworkSender<In>, crate::block::CoordHasherBuilder>;
}

/// This struct is used to index inside the `typemap` with the `DemultiplexingReceiver`s.
struct DemultiplexingReceiverKey<In: ExchangeData>(PhantomData<In>);

impl<In: ExchangeData> TypeMapKey for DemultiplexingReceiverKey<In> {
    type Value = HashMap<DemuxCoord, DemuxHandle<In>, crate::block::CoordHasherBuilder>;
}

/// This struct is used to index inside the `typemap` with the `MultiplexingSender`s.
struct MultiplexingSenderKey<In: ExchangeData>(PhantomData<In>);

impl<In: ExchangeData> TypeMapKey for MultiplexingSenderKey<In> {
    type Value = HashMap<DemuxCoord, MultiplexingSender<In>, crate::block::CoordHasherBuilder>;
}

/// Metadata about a registered sender.
#[derive(Default, Derivative)]
#[derivative(Debug)]
struct SenderMetadata {
    /// This sender should connect to the remote recipient.
    to_remote: bool,
}

/// This struct keeps track of the network topology, all the registered replicas and their
/// connections.
pub(crate) struct NetworkTopology {
    /// Configuration of the environment.
    config: Arc<RuntimeConfig>,
    /// All the registered receivers.
    ///
    /// Since the `NetworkReceiver` is generic over the element type we cannot simply store them
    /// (they have essentially different types). So instead of storing them in a
    /// `HashMap<Coord, NetworkReceiver<???>>` we store them indexed by element type inside a
    /// typemap: `TypeMap<ItemType -> HashMap<Coord, NetworkReceiver<ItemType>>`.
    ///
    /// This forces us to access the map only where we know the type of the element type, so some
    /// trickery is required to, for example, start the network component of all the receivers.
    /// At registration all the tasks start and immediately wait on a channel. We then store that
    /// channel (that does not depend of the element type) and later we use it to tell the task to
    /// actually start or exit.
    ///
    /// This map is indexed using `ReceiverKey`.
    receivers: Option<TypeMap>,
    /// All the registered local senders.
    ///
    /// It works exactly like `self.receivers`.
    ///
    /// This map is indexed using `SenderKey`.
    senders: Option<TypeMap>,

    /// All the registered demultiplexers.
    ///
    /// This map is indexed using `DemultiplexingReceiverKey`.
    demultiplexers: Option<TypeMap>,
    /// All the registered multiplexers.
    ///
    /// This map is indexed using `MultiplexingSenderKey`.
    multiplexers: Option<TypeMap>,

    /// The adjacency list of the execution graph.
    next: HashMap<(Coord, TypeId), Vec<(Coord, bool)>, crate::block::CoordHasherBuilder>,
    /// The inverse adjacency list of the execution graph.
    prev: HashMap<Coord, Vec<(Coord, TypeId)>, crate::block::CoordHasherBuilder>,
    /// The metadata about all the registered senders.
    senders_metadata: HashMap<ReceiverEndpoint, SenderMetadata, crate::block::CoordHasherBuilder>,
    /// The list of all the replicas, indexed by block.
    block_replicas: HashMap<BlockId, HashSet<Coord>, crate::block::CoordHasherBuilder>,

    /// The set of the used receivers.
    ///
    /// This set makes sure that a given receiver is not initialized twice. Bad things may happen if
    /// the same receiver is initialized twice (the same socket may be bound twice).
    used_receivers: HashSet<ReceiverEndpoint>,
    /// The set of the registered endpoints.
    ///
    /// This just makes sure no endpoint is registered twice.
    registered_receivers: HashSet<ReceiverEndpoint>,

    /// The mapping between the coordinate of a demultiplexer of a block to the actual address/port
    /// of that demultiplexer in the network.
    demultiplexer_addresses: HashMap<DemuxCoord, (String, u16), crate::block::CoordHasherBuilder>,

    /// The set of join handles of the various threads spawned by the topology.
    #[cfg(not(feature = "tokio"))]
    join_handles: Vec<JoinHandle<()>>,

    #[cfg(feature = "tokio")]
    async_join_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for NetworkTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkTopology")
            .field("config", self.config.as_ref())
            .field("next", &self.next)
            .field("senders_metadata", &self.senders_metadata)
            .field("block_replicas", &self.block_replicas)
            .field("demultiplexer_addresses", &self.demultiplexer_addresses)
            .finish()
    }
}

impl NetworkTopology {
    pub(crate) fn new(config: Arc<RuntimeConfig>) -> Self {
        NetworkTopology {
            config,
            receivers: Some(TypeMap::new()),
            senders: Some(TypeMap::new()),
            demultiplexers: Some(TypeMap::new()),
            multiplexers: Some(TypeMap::new()),
            next: Default::default(),
            prev: Default::default(),
            senders_metadata: Default::default(),
            block_replicas: Default::default(),
            used_receivers: Default::default(),
            registered_receivers: Default::default(),
            demultiplexer_addresses: Default::default(),
            #[cfg(not(feature = "tokio"))]
            join_handles: Default::default(),
            #[cfg(feature = "tokio")]
            async_join_handles: Default::default(),
        }
    }

    #[cfg(feature = "tokio")]
    /// Knowing that the computation ended, tear down the topology wait for all of its thread to
    /// exit.
    pub(crate) async fn stop_and_wait(&mut self) {
        self.async_join_handles
            .drain(..)
            .collect::<futures::stream::FuturesUnordered<_>>()
            .for_each(|h| {
                h.unwrap();
                futures::future::ready(())
            })
            .await;
    }

    #[cfg(not(feature = "tokio"))]
    /// Knowing that the computation ended, tear down the topology wait for all of its thread to
    /// exit.
    pub(crate) fn stop_and_wait(&mut self) {
        for handle in self.join_handles.drain(..) {
            handle.join().unwrap();
        }
    }

    /// Get all the outgoing senders from a replica.
    ///
    /// If a replica has more that one _outgoing_ type this method must not be used, but separate
    /// calls to `get_sender` should be done.
    pub fn get_senders<T: ExchangeData>(
        &mut self,
        coord: Coord,
    ) -> Vec<(ReceiverEndpoint, NetworkSender<T>)> {
        let typ = TypeId::of::<T>();
        match self.next.get(&(coord, typ)) {
            None => Default::default(),
            Some(next) => {
                let next = next.clone();
                next.iter()
                    .filter_map(|&(c, fragile)| {
                        if fragile {
                            None
                        } else {
                            let receiver_endpoint = ReceiverEndpoint::new(c, coord.block_id);
                            Some((receiver_endpoint, self.get_sender(receiver_endpoint)))
                        }
                    })
                    .collect()
            }
        }
    }

    /// Get the sender associated with a given receiver endpoint. This may register a new channel if
    /// it was not registered before.
    pub fn get_sender<T: ExchangeData>(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
    ) -> NetworkSender<T> {
        if !self
            .senders
            .as_mut()
            .unwrap()
            .contains_key::<SenderKey<T>>()
        {
            self.senders
                .as_mut()
                .unwrap()
                .insert::<SenderKey<T>>(Default::default());
        }
        let entry = self
            .senders
            .as_mut()
            .unwrap()
            .get_mut::<SenderKey<T>>()
            .unwrap();
        if !entry.contains_key(&receiver_endpoint) {
            self.register_channel::<T>(receiver_endpoint);
        }
        self.senders
            .as_mut()
            .unwrap()
            .get::<SenderKey<T>>()
            .unwrap()
            .get(&receiver_endpoint)
            .unwrap()
            .clone()
    }

    /// Get the receiver of all the ingoing messages to a replica endpoint. This may register a new
    /// channel if it was not registered before.
    ///
    /// Calling this function twice with the same parameter will panic.
    pub fn get_receiver<T: ExchangeData>(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
    ) -> NetworkReceiver<T> {
        if self.used_receivers.contains(&receiver_endpoint) {
            panic!("The receiver for {receiver_endpoint} has already been got",);
        }
        self.used_receivers.insert(receiver_endpoint);

        let entry = self
            .receivers
            .as_mut()
            .unwrap()
            .entry::<ReceiverKey<T>>()
            .or_default();
        // if the channel has not been registered yet, register it
        if !entry.contains_key(&receiver_endpoint) {
            self.register_channel::<T>(receiver_endpoint);
        }
        self.receivers
            .as_mut()
            .unwrap()
            .get_mut::<ReceiverKey<T>>()
            .unwrap()
            .remove(&receiver_endpoint)
            .unwrap()
    }

    fn register_demux<T: ExchangeData>(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
        local_sender: Sender<NetworkMessage<T>>,
    ) {
        let demux_coord = DemuxCoord::from(receiver_endpoint);
        let demuxes = self
            .demultiplexers
            .as_mut()
            .unwrap()
            .entry::<DemultiplexingReceiverKey<T>>()
            .or_default();

        if let Entry::Vacant(e) = demuxes.entry(demux_coord) {
            // find the set of all the previous blocks that have a MultiplexingSender that
            // point to this DemultiplexingReceiver.
            let mut prev = HashSet::new();
            // FIXME: maybe this can benefit from self.prev
            for (&(from, typ), to) in &self.next {
                // local blocks won't use the multiplexer
                if from.host_id == demux_coord.coord.host_id {
                    continue;
                }
                // ignore channels of the wrong type
                if typ != TypeId::of::<T>() {
                    continue;
                }
                for &(to, _fragile) in to {
                    if demux_coord.includes_channel(from, to) {
                        prev.insert(BlockCoord::from(from));
                    }
                }
            }
            if !prev.is_empty() {
                let address = self.demultiplexer_addresses[&demux_coord].clone();
                let (demux, join_handle) = DemuxHandle::new(demux_coord, address, prev.len());
                #[cfg(not(feature = "tokio"))]
                self.join_handles.push(join_handle);
                #[cfg(feature = "tokio")]
                self.async_join_handles.push(join_handle);
                e.insert(demux);
            } else {
                log::debug!("demux {} skipping (no remote predecessors)", demux_coord);
            }
        }
        if let Some(demux) = demuxes.get_mut(&demux_coord) {
            demux.register(receiver_endpoint, local_sender)
        }
    }

    fn register_mux<T: ExchangeData>(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
    ) -> NetworkSender<T> {
        let muxers = self
            .multiplexers
            .as_mut()
            .unwrap()
            .entry::<MultiplexingSenderKey<T>>()
            .or_default();
        let demux_coord = DemuxCoord::from(receiver_endpoint);

        if let Entry::Vacant(e) = muxers.entry(demux_coord) {
            let address = self.demultiplexer_addresses[&demux_coord].clone();
            let (mux, join_handle) = MultiplexingSender::new(demux_coord, address);
            #[cfg(not(feature = "tokio"))]
            self.join_handles.push(join_handle);
            #[cfg(feature = "tokio")]
            self.async_join_handles.push(join_handle);
            e.insert(mux);
        }
        muxers
            .get_mut(&demux_coord)
            .unwrap()
            .get_sender(receiver_endpoint)
    }

    /// Register the channel for the given receiver.
    ///
    /// This will initialize both the sender and the receiver to the receiver. If it's appropriate
    /// also the multiplexer and/or the demultiplexer are initialized and started.
    fn register_channel<T: ExchangeData>(&mut self, receiver_endpoint: ReceiverEndpoint) {
        log::debug!("new endpoint {}", receiver_endpoint);
        assert!(
            !self.registered_receivers.contains(&receiver_endpoint),
            "receiver {receiver_endpoint} has already been registered",
        );
        self.registered_receivers.insert(receiver_endpoint);

        let sender_metadata = self
            .senders_metadata
            .get(&receiver_endpoint)
            .unwrap_or_else(|| panic!("Channel for endpoint {receiver_endpoint} not registered",));

        match self.config.as_ref() {
            RuntimeConfig::Remote(_) => {
                if sender_metadata.to_remote {
                    let sender = self.register_mux(receiver_endpoint);

                    self.senders
                        .as_mut()
                        .unwrap()
                        .entry::<SenderKey<T>>()
                        .or_default()
                        .insert(receiver_endpoint, sender);
                } else {
                    let (sender, receiver) = local_channel(receiver_endpoint);

                    if receiver_endpoint.coord.host_id == self.config.host_id().unwrap() {
                        self.register_demux(receiver_endpoint, sender.clone_inner());
                    }

                    self.receivers
                        .as_mut()
                        .unwrap()
                        .entry::<ReceiverKey<T>>()
                        .or_default()
                        .insert(receiver_endpoint, receiver);
                    self.senders
                        .as_mut()
                        .unwrap()
                        .entry::<SenderKey<T>>()
                        .or_default()
                        .insert(receiver_endpoint, sender);
                };
            }
            RuntimeConfig::Local(_) => {
                let (sender, receiver) = local_channel(receiver_endpoint);

                self.receivers
                    .as_mut()
                    .unwrap()
                    .entry::<ReceiverKey<T>>()
                    .or_default()
                    .insert(receiver_endpoint, receiver);
                self.senders
                    .as_mut()
                    .unwrap()
                    .entry::<SenderKey<T>>()
                    .or_default()
                    .insert(receiver_endpoint, sender);
            }
        }
    }

    /// Register the connection between two replicas.
    ///
    /// If the 2 replicas are remote this function saves the connection between them, but does not
    /// prepare any sender/receiver for that channel.
    ///
    /// This will not actually bind/connect sockets, it will just mark them as bindable/connectable.
    ///
    /// If `fragile` is set to `true`, this connection won't be available with `get_senders` but
    /// only with `get_sender`.
    pub fn connect(&mut self, from: Coord, to: Coord, typ: TypeId, fragile: bool) {
        let host_id = self.config.host_id().unwrap();
        let from_remote = from.host_id != host_id;
        let to_remote = to.host_id != host_id;

        log::trace!(
            "new connection: {} -> {}, remote: ({}, {})",
            from,
            to,
            from_remote,
            to_remote
        );
        self.next
            .entry((from, typ))
            .or_default()
            .push((to, fragile));
        self.prev.entry(to).or_default().push((from, typ));

        // save the replicas
        self.block_replicas
            .entry(from.block_id)
            .or_default()
            .insert(from);
        self.block_replicas
            .entry(to.block_id)
            .or_default()
            .insert(to);

        if from_remote && to_remote {
            // totally remote channels are not interesting for this host, but they need to be
            // considered in `self.next` for a deterministic assignment of the demultiplexer ports.
            return;
        }

        let receiver_endpoint = ReceiverEndpoint::new(to, from.block_id);
        self.senders_metadata.entry(receiver_endpoint).or_default();

        // we want to connect to a remote: this sender should be remote
        if to_remote {
            let metadata = self.senders_metadata.get_mut(&receiver_endpoint).unwrap();
            metadata.to_remote = true;
        }
    }

    /// The list of previous replicas of a given replica.
    pub fn prev(&self, coord: Coord) -> Vec<(Coord, TypeId)> {
        if let Some(prev) = self.prev.get(&coord) {
            prev.clone()
        } else {
            vec![]
        }
    }

    /// Return the list of all the replicas of a block.
    pub fn replicas(&self, block_id: BlockId) -> Vec<Coord> {
        if let Some(replicas) = self.block_replicas.get(&block_id) {
            replicas.iter().cloned().collect()
        } else {
            vec![]
        }
    }

    /// Finalize the network topology setting up internal properties.
    ///
    /// This has to be called after all the calls to `connect` and before any call to `get_senders`
    /// and `get_receiver`.
    ///
    /// Internally this computes the mapping between `DemuxCoord` and actual TCP port.
    pub fn build(&mut self) {
        log::debug!("finalizing topology");
        // Close handles to multiplexers to start the worker threads

        let config = if let RuntimeConfig::Remote(config) = self.config.as_ref() {
            config
        } else {
            return;
        };
        let mut coords = IndexSet::new();
        for (&(from, _typ), to) in self.next.iter() {
            for &(to, _fragile) in to {
                let coord = DemuxCoord::new(from, to);
                coords.insert(coord);
            }
        }
        coords.sort();
        let mut used_ports: HashMap<HostId, u16> = HashMap::new();
        // sort the coords in order to have a deterministic assignment between all the hosts
        for coord in coords.into_iter() {
            let host_id = coord.coord.host_id;
            let port_offset = used_ports.entry(host_id).or_default();
            let host = &config.hosts[host_id as usize];
            let port = host.base_port + *port_offset;
            *port_offset += 1;
            let address = (host.address.clone(), port);
            log::debug!("demux {} socket: {:?}", coord, address);
            self.demultiplexer_addresses.insert(coord, address);
        }
    }

    /// Finalize the topology and start mutliplexers and demultiplexers
    pub fn finalize(&mut self) {
        // drop all the senders/receivers making sure no dangling sender keep alive their network
        // receivers.
        self.receivers.take();
        self.senders.take();

        self.demultiplexers.take();
        self.multiplexers.take();
    }

    pub fn log(&self) {
        let mut topology = "execution graph:".to_owned();
        let mut sorted = self.next.iter().collect::<Vec<_>>();
        sorted.sort();
        for ((coord, _typ), next) in sorted {
            write!(&mut topology, "\n  {coord}:",).unwrap();
            let mut next_sorted = next.iter().collect::<Vec<_>>();
            next_sorted.sort();
            for (next, fragile) in next_sorted {
                write!(
                    &mut topology,
                    " {}{}",
                    next,
                    if *fragile { "*" } else { "" }
                )
                .unwrap();
            }
        }
        log::trace!("{}", topology);
    }
}

#[cfg(test)]
mod tests {
    use crate::network::NetworkMessage;
    use crate::operator::StreamElement;

    use super::*;

    fn build_message<T>(t: T) -> NetworkMessage<T> {
        NetworkMessage::new_single(StreamElement::Item(t), Coord::default())
    }

    #[test]
    fn test_local_topology() {
        let config = Arc::new(RuntimeConfig::local(4).unwrap());
        let mut topology = NetworkTopology::new(config);

        // s1 [b0, h0] -> r1 [b2, h0] (endpoint 1) type=i32
        // s2 [b1, h0] -> r1 [b2, h0] (endpoint 2) type=u64
        // s2 [b1, h0] -> r2 [b3, h0] (endpoint 3) type=u64

        let sender1 = Coord::new(0, 0, 0);
        let sender2 = Coord::new(1, 0, 0);
        let receiver1 = Coord::new(2, 0, 0);
        let receiver2 = Coord::new(3, 0, 0);

        topology.connect(sender1, receiver1, TypeId::of::<i32>(), false);
        topology.connect(sender2, receiver1, TypeId::of::<u64>(), false);
        topology.connect(sender2, receiver2, TypeId::of::<u64>(), false);
        topology.build();

        let endpoint1 = ReceiverEndpoint::new(receiver1, 0);
        let endpoint2 = ReceiverEndpoint::new(receiver1, 1);
        let endpoint3 = ReceiverEndpoint::new(receiver2, 1);

        let tx1 = topology
            .get_senders::<i32>(sender1)
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(tx1.len(), 1);
        tx1[&endpoint1].send(build_message(123i32)).unwrap();

        let tx2 = topology
            .get_senders::<u64>(sender2)
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(tx2.len(), 2);
        tx2[&endpoint2].send(build_message(666u64)).unwrap();
        tx2[&endpoint3].send(build_message(42u64)).unwrap();

        let rx1 = topology.get_receiver::<i32>(endpoint1);
        assert_eq!(
            rx1.recv().unwrap().into_iter().collect::<Vec<_>>(),
            vec![StreamElement::Item(123i32)]
        );

        let rx2 = topology.get_receiver::<u64>(endpoint2);
        assert_eq!(
            rx2.recv().unwrap().into_iter().collect::<Vec<_>>(),
            vec![StreamElement::Item(666u64)]
        );

        let rx3 = topology.get_receiver::<u64>(endpoint3);
        assert_eq!(
            rx3.recv().unwrap().into_iter().collect::<Vec<_>>(),
            vec![StreamElement::Item(42u64)]
        );
    }

    #[cfg(not(feature = "tokio"))]
    #[test]
    fn test_remote_topology() {
        use crate::config::ConfigBuilder;

        let mut toml_path = tempfile::NamedTempFile::new().unwrap();
        let config_toml = r#"[[host]]
address = "127.0.0.1"
base_port = 21841
num_cores = 1
[[host]]
address = "127.0.0.1"
base_port = 31258
num_cores = 1
"#;
        std::io::Write::write_all(&mut toml_path, config_toml.as_bytes()).unwrap();

        // s1 [b0, h0, r0] -> r1 [b2, h1, r0] (endpoint 1) type=i32
        // s2 [b0, h1, r0] -> r1 [b2, h1, r0] (endpoint 1) type=i32
        // s3 [b1, h0, r0] -> r1 [b2, h1, r0] (endpoint 2) type=u64
        // s4 [b1, h0, r1] -> r1 [b2, h1, r0] (endpoint 2) type=u64
        // s3 [b1, h0, r0] -> r2 [b2, h1, r1] (endpoint 3) type=u64
        // s4 [b1, h0, r1] -> r2 [b2, h1, r1] (endpoint 3) type=u64

        let run = |config: Arc<RuntimeConfig>| {
            let host = config.host_id().unwrap();
            let mut topology = NetworkTopology::new(config);

            let s1 = Coord::new(0, 0, 0);
            let s2 = Coord::new(0, 1, 0);
            let s3 = Coord::new(1, 0, 0);
            let s4 = Coord::new(1, 0, 1);

            let r1 = Coord::new(2, 1, 0);
            let r2 = Coord::new(2, 1, 1);

            topology.connect(s1, r1, TypeId::of::<i32>(), false);
            topology.connect(s2, r1, TypeId::of::<i32>(), false);
            topology.connect(s3, r1, TypeId::of::<u64>(), false);
            topology.connect(s4, r1, TypeId::of::<u64>(), false);
            topology.connect(s3, r2, TypeId::of::<u64>(), false);
            topology.connect(s4, r2, TypeId::of::<u64>(), false);
            topology.build();

            let endpoint1 = ReceiverEndpoint::new(r1, 0);
            let endpoint2 = ReceiverEndpoint::new(r1, 1);
            let endpoint3 = ReceiverEndpoint::new(r2, 1);

            if s1.host_id == host {
                let tx1 = topology
                    .get_senders::<i32>(s1)
                    .into_iter()
                    .collect::<HashMap<_, _>>();
                tx1[&endpoint1].send(build_message(123i32)).unwrap();
            }

            if s2.host_id == host {
                let tx2 = topology
                    .get_senders::<i32>(s2)
                    .into_iter()
                    .collect::<HashMap<_, _>>();
                assert_eq!(tx2.len(), 1);
                tx2[&endpoint1].send(build_message(456i32)).unwrap();
            }

            if s3.host_id == host {
                let tx3 = topology
                    .get_senders::<u64>(s3)
                    .into_iter()
                    .collect::<HashMap<_, _>>();
                assert_eq!(tx3.len(), 2);
                tx3[&endpoint2].send(build_message(666u64)).unwrap();
                tx3[&endpoint3].send(build_message(42u64)).unwrap();
            }

            if s4.host_id == host {
                let tx4 = topology
                    .get_senders::<u64>(s4)
                    .into_iter()
                    .collect::<HashMap<_, _>>();
                assert_eq!(tx4.len(), 2);
                tx4[&endpoint2].send(build_message(111u64)).unwrap();
                tx4[&endpoint3].send(build_message(4242u64)).unwrap();
            }

            let mut join_handles = vec![];

            if endpoint1.coord.host_id == host {
                let rx1 = topology.get_receiver::<i32>(endpoint1);
                join_handles.push(
                    std::thread::Builder::new()
                        .name("rx1".into())
                        .spawn(move || receiver(rx1, vec![123i32, 456i32]))
                        .unwrap(),
                );
            }

            if endpoint2.coord.host_id == host {
                let rx2 = topology.get_receiver::<u64>(endpoint2);
                join_handles.push(
                    std::thread::Builder::new()
                        .name("rx2".into())
                        .spawn(move || receiver(rx2, vec![111u64, 666u64]))
                        .unwrap(),
                );
            }

            if endpoint3.coord.host_id == host {
                let rx3 = topology.get_receiver::<u64>(endpoint3);
                join_handles.push(
                    std::thread::Builder::new()
                        .name("rx3".into())
                        .spawn(move || receiver(rx3, vec![42u64, 4242u64]))
                        .unwrap(),
                );
            }

            topology.finalize();

            for handle in join_handles {
                handle.join().unwrap();
            }
            topology.stop_and_wait();
        };

        let config0 = Arc::new(
            ConfigBuilder::new_remote()
                .parse_file(toml_path.path())
                .unwrap()
                .host_id(0)
                .build()
                .unwrap(),
        );

        let config1 = Arc::new(
            ConfigBuilder::new_remote()
                .parse_file(toml_path.path())
                .unwrap()
                .host_id(1)
                .build()
                .unwrap(),
        );

        let join0 = std::thread::Builder::new()
            .name("host0".into())
            .spawn(move || run(config0))
            .unwrap();
        let join1 = std::thread::Builder::new()
            .name("host1".into())
            .spawn(move || run(config1))
            .unwrap();

        join0.join().unwrap();
        join1.join().unwrap();
    }

    #[cfg(not(feature = "tokio"))]
    fn receiver<T: ExchangeData + Ord + std::fmt::Debug>(
        receiver: NetworkReceiver<T>,
        expected: Vec<T>,
    ) {
        let mut res = (0..expected.len())
            .flat_map(|_| receiver.recv().unwrap().into_iter())
            .collect::<Vec<_>>();
        res.sort_unstable();
        assert_eq!(
            res,
            expected
                .into_iter()
                .map(StreamElement::Item)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_multiple_output_types() {
        let config = Arc::new(RuntimeConfig::local(4).unwrap());
        let mut topology = NetworkTopology::new(config);

        // s1 [b0, h0] -> r1 [b1, h0] (endpoint 1) type=i32
        // s2 [b0, h0] -> r2 [b2, h0] (endpoint 2) type=u64

        let sender1 = Coord::new(0, 0, 0);
        let sender2 = Coord::new(0, 0, 0);
        let receiver1 = Coord::new(1, 0, 0);
        let receiver2 = Coord::new(2, 0, 0);

        topology.connect(sender1, receiver1, TypeId::of::<i32>(), false);
        topology.connect(sender2, receiver2, TypeId::of::<u64>(), false);
        topology.build();

        let endpoint1 = ReceiverEndpoint::new(receiver1, 0);
        let endpoint2 = ReceiverEndpoint::new(receiver2, 0);

        let tx1 = topology.get_sender::<i32>(endpoint1);
        tx1.send(build_message(123i32)).unwrap();

        let tx2 = topology.get_sender::<u64>(endpoint2);
        tx2.send(build_message(666u64)).unwrap();

        let rx1 = topology.get_receiver::<i32>(endpoint1);
        assert_eq!(
            rx1.recv().unwrap().into_iter().collect::<Vec<_>>(),
            vec![StreamElement::Item(123i32)]
        );

        let rx2 = topology.get_receiver::<u64>(endpoint2);
        assert_eq!(
            rx2.recv().unwrap().into_iter().collect::<Vec<_>>(),
            vec![StreamElement::Item(666u64)]
        );
    }
}
