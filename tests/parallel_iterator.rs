use itertools::Itertools;
use renoir::operator::source::ParallelIteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn parallel_iterator() {
    TestHelper::local_remote_env(|env| {
        let n = 100u64;
        let source = ParallelIteratorSource::new(move |id, instances| {
            let chunk_size = n.div_ceil(instances);
            let remaining = n - n.min(chunk_size * id);
            let range = remaining.min(chunk_size);

            let start = id * chunk_size;
            let stop = id * chunk_size + range;
            start..stop
        });
        let res = env.stream(source).shuffle().collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            let expected = (0..n).collect_vec();
            assert_eq!(res, expected);
        }
    });
}
