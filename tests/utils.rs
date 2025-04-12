#![allow(dead_code)] // not all tests use all the members

use std::fmt::Display;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::Duration;

use itertools::{process_results, Itertools};

use rand::{rng, Rng};
use renoir::config::{ConfigBuilder, HostConfig, RuntimeConfig};
use renoir::operator::{Data, Operator, StreamElement, Timestamp};
use renoir::structure::BlockStructure;
use renoir::CoordUInt;
use renoir::ExecutionMetadata;
use renoir::StreamContext;

/// Port from which the integration tests start allocating sockets for the remote runtime.
const TEST_BASE_PORT: u16 = 17666;

/// The index of the current test.
///
/// It will be used to generate unique local IP addresses for each test.
static TEST_INDEX: AtomicU16 = AtomicU16::new(0);

/// A fake operator that makes sure the watermarks are consistent with the data.
///
/// This will check that no data has a timestamp lower or equal than a previous watermark. This also
/// keeps track of the number of watermarks received.
#[derive(Clone)]
pub struct WatermarkChecker<Out: Data, PreviousOperator>
where
    PreviousOperator: Operator<Out = Out>,
{
    last_watermark: Option<Timestamp>,
    prev: PreviousOperator,
    received_watermarks: Arc<AtomicUsize>,
    _out: PhantomData<Out>,
}

impl<Out: Data, PreviousOperator> Display for WatermarkChecker<Out, PreviousOperator>
where
    PreviousOperator: Operator<Out = Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WatermarkChecker")
    }
}

impl<Out: Data, PreviousOperator: Operator<Out = Out>> WatermarkChecker<Out, PreviousOperator> {
    pub fn new(prev: PreviousOperator, received_watermarks: Arc<AtomicUsize>) -> Self {
        Self {
            last_watermark: None,
            prev,
            received_watermarks,
            _out: Default::default(),
        }
    }
}

impl<Out: Data, PreviousOperator: Operator<Out = Out>> Operator
    for WatermarkChecker<Out, PreviousOperator>
{
    type Out = Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        let item = self.prev.next();
        match &item {
            StreamElement::Timestamped(_, ts) => {
                if let Some(w) = &self.last_watermark {
                    assert!(ts > w);
                }
            }
            StreamElement::Watermark(ts) => {
                if let Some(w) = &self.last_watermark {
                    assert!(ts > w);
                }
                self.last_watermark = Some(*ts);
                self.received_watermarks.fetch_add(1, Ordering::Release);
            }
            _ => {}
        }
        item
    }

    fn structure(&self) -> BlockStructure {
        Default::default()
    }
}

/// Helper functions for running the integration tests.
///
/// For now this is in the public undocumented API and not in the integration test crate because it
/// has to craft the configuration as they come from the network.
pub struct TestHelper;

impl TestHelper {
    fn setup() {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .parse_default_env()
            .try_init();
    }

    /// Crate an environment with the specified config and run the test build by `body`.
    pub fn env_with_config(config: RuntimeConfig, body: Arc<dyn Fn(StreamContext) + Send + Sync>) {
        let timeout_sec = Self::parse_int_from_env("RSTREAM_TEST_TIMEOUT").unwrap_or(10);
        let timeout = Duration::from_secs(timeout_sec);
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("Worker".into())
            .spawn(move || {
                let env = StreamContext::new(config);
                body(env);
                sender.send(()).unwrap();
            })
            .unwrap();
        match receiver.recv_timeout(timeout) {
            Ok(_) => {}
            Err(RecvTimeoutError::Timeout) => {
                panic!("Worker thread didn't complete before the timeout of {timeout:?}");
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("Worker thread has panicked!");
            }
        }
        worker.join().expect("Worker thread has panicked!");
    }

    /// Run the test body under a local environment.
    pub fn local_env(body: Arc<dyn Fn(StreamContext) + Send + Sync>, num_cores: CoordUInt) {
        Self::setup();
        let config = RuntimeConfig::local(num_cores).unwrap();
        log::debug!("Running test with env: {:?}", config);
        Self::env_with_config(config, body)
    }

    /// Run the test body under a simulated remote environment.
    pub fn remote_env(
        body: Arc<dyn Fn(StreamContext) + Send + Sync>,
        num_hosts: CoordUInt,
        cores_per_host: CoordUInt,
    ) {
        Self::setup();
        let mut hosts = vec![];
        let test_id: u16 = rng().random(); //TEST_INDEX.fetch_add(1, Ordering::SeqCst) + 1;
        for host_id in 0..num_hosts {
            let high_part = (test_id & 0xff00) >> 8;
            let low_part = test_id & 0xff;
            let address = format!("127.{high_part}.{low_part}.{host_id}");
            hosts.push(HostConfig {
                address,
                base_port: TEST_BASE_PORT,
                num_cores: cores_per_host,
                ssh: Default::default(),
                perf_path: None,
            });
        }

        let mut join_handles = vec![];
        for host_id in 0..num_hosts {
            let config = ConfigBuilder::new_remote()
                .add_hosts(&hosts)
                .host_id(host_id)
                .build()
                .unwrap();
            let body = body.clone();
            join_handles.push(
                std::thread::Builder::new()
                    .name(format!("Test host{host_id}"))
                    .spawn(move || Self::env_with_config(config, body))
                    .unwrap(),
            )
        }
        for (host_id, handle) in join_handles.into_iter().enumerate() {
            handle
                .join()
                .unwrap_or_else(|e| panic!("Remote worker for host {host_id} crashed: {e:?}"));
        }
    }

    /// Run the test body under a local environment and later under a simulated remote environment.
    pub fn local_remote_env<F>(body: F)
    where
        F: Fn(StreamContext) + Send + Sync + 'static,
    {
        let body = Arc::new(body);

        let local_cores =
            Self::parse_list_from_env("RSTREAM_TEST_LOCAL_CORES").unwrap_or_else(|| vec![4]);
        for num_cores in local_cores {
            Self::local_env(body.clone(), num_cores);
        }

        let remote_hosts =
            Self::parse_list_from_env("RSTREAM_TEST_REMOTE_HOSTS").unwrap_or_else(|| vec![4]);
        let remote_cores =
            Self::parse_list_from_env("RSTREAM_TEST_REMOTE_CORES").unwrap_or_else(|| vec![4]);
        for num_hosts in remote_hosts {
            for &num_cores in &remote_cores {
                Self::remote_env(body.clone(), num_hosts, num_cores);
            }
        }
    }

    /// Parse a list of arguments from an environment variable.
    ///
    /// The list should be comma separated without spaces.
    fn parse_list_from_env(var_name: &str) -> Option<Vec<CoordUInt>> {
        let content = std::env::var(var_name).ok()?;
        if content.is_empty() {
            return Some(Vec::new());
        }
        let values = content.split(',').map(CoordUInt::from_str).collect_vec();
        process_results(values, |values| values.collect_vec()).ok()
    }

    fn parse_int_from_env(var_name: &str) -> Option<u64> {
        let content = std::env::var(var_name).ok()?;
        u64::from_str(&content).ok()
    }
}
