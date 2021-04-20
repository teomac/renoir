use itertools::Itertools;

use rstream::operator::source::IteratorSource;
use rstream::test::TestHelper;

#[test]
fn unkey_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .key_by(|&n| n.to_string())
            .unkey()
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..10u8).map(|n| (n.to_string(), n)).collect_vec();
            assert_eq!(res, expected);
        }
    });
}
