#![allow(unused)]

use criterion::{black_box, Bencher};
use renoir::config::{ConfigBuilder, HostConfig, RemoteConfig, RuntimeConfig};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use renoir::*;

pub const SAMPLES: usize = 50;

static NONCE: AtomicU16 = AtomicU16::new(1);
const PORT_BASE: u16 = 9090;

pub fn remote_loopback_deploy(
    num_hosts: CoordUInt,
    cores_per_host: CoordUInt,
    body: impl Fn(&StreamContext) + Send + Sync + 'static,
) {
    let mut hosts = vec![];
    for host_id in 0..num_hosts {
        let test_id = NONCE.fetch_add(1, Ordering::SeqCst);
        let [hi, lo] = test_id.to_be_bytes();
        let address = format!("127.{hi}.{lo}.{host_id}");
        hosts.push(HostConfig {
            address,
            base_port: PORT_BASE,
            num_cores: cores_per_host,
            ssh: Default::default(),
            perf_path: None,
        });
    }

    let mut join_handles = vec![];
    let body = Arc::new(body);
    for host_id in 0..num_hosts {
        let config = ConfigBuilder::new_remote()
            .add_hosts(&hosts)
            .host_id(host_id)
            .build()
            .unwrap();

        let body = body.clone();
        join_handles.push(
            std::thread::Builder::new()
                .name(format!("lohost-{host_id:02}"))
                .spawn(move || {
                    let env = StreamContext::new(config);
                    body(&env);
                    env.execute_blocking();
                })
                .unwrap(),
        )
    }
    for (host_id, handle) in join_handles.into_iter().enumerate() {
        handle
            .join()
            .unwrap_or_else(|e| panic!("Remote worker for host {host_id} crashed: {e:?}"));
    }
}

pub struct BenchBuilder<F, G, R>
where
    F: Fn() -> StreamContext,
    G: Fn(&StreamContext) -> R,
{
    make_env: F,
    make_network: G,
    _result: PhantomData<R>,
}

impl<F, G, R> BenchBuilder<F, G, R>
where
    F: Fn() -> StreamContext,
    G: Fn(&StreamContext) -> R,
{
    pub fn new(make_env: F, make_network: G) -> Self {
        Self {
            make_env,
            make_network,
            _result: Default::default(),
        }
    }

    pub fn bench(&self, n: u64) -> Duration {
        let mut time = Duration::default();
        for _ in 0..n {
            let mut ctx = (self.make_env)();
            let _result = (self.make_network)(&mut ctx);
            let start = Instant::now();
            ctx.execute_blocking();
            time += start.elapsed();
            black_box(_result);
        }
        time
    }

    #[cfg(feature = "tokio")]
    pub fn bench_tokio(&self, n: u64) -> Duration {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let mut time = Duration::default();
                for _ in 0..n {
                    let mut ctx = (self.make_env)();
                    let _result = (self.make_network)(&mut ctx);
                    let start = Instant::now();
                    ctx.execute().await;
                    time += start.elapsed();
                    black_box(_result);
                }
                time
            })
    }
}

pub fn renoir_bench_default(b: &mut Bencher, logic: impl Fn(&StreamContext)) {
    let builder = BenchBuilder::new(StreamContext::new_local, logic);
    b.iter_custom(|n| builder.bench(n));
}

#[cfg(feature = "tokio")]
pub fn renoir_bench_default_tokio(b: &mut Bencher, logic: impl Fn(&StreamContext)) {
    let builder = BenchBuilder::new(StreamContext::new_local, logic);
    b.iter_custom(|n| builder.bench_tokio(n));
}
