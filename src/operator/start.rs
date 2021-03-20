use std::collections::VecDeque;

use crate::network::{NetworkMessage, NetworkReceiver};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Debug, Derivative)]
#[derivative(Clone, Default(bound = ""))]
pub struct StartBlock<Out: Data> {
    metadata: Option<ExecutionMetadata>,
    #[derivative(Clone(clone_with = "clone_none"))]
    receiver: Option<NetworkReceiver<NetworkMessage<Out>>>,
    buffer: VecDeque<StreamElement<Out>>,
    missing_ends: usize,
    /// The last call to next caused a timeout, so a FlushBatch has been emitted. In this case this
    /// field is set to true to avoid flooding with FlushBatch.
    already_timed_out: bool,
}

impl<Out: Data> Operator<Out> for StartBlock<Out> {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let mut network = metadata.network.lock().unwrap();
        self.receiver = Some(network.get_receiver(metadata.coord));
        drop(network);
        self.missing_ends = metadata.num_prev;
        info!(
            "StartBlock {} initialized, {} previous blocks, receiver is: {:?}",
            metadata.coord, metadata.num_prev, self.receiver
        );
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        let metadata = self.metadata.as_ref().unwrap();
        // all the previous blocks sent and end: we're done
        if self.missing_ends == 0 {
            info!("StartBlock for {} has ended", metadata.coord);
            return StreamElement::End;
        }
        let receiver = self.receiver.as_ref().unwrap();
        if self.buffer.is_empty() {
            let max_delay = metadata.batch_mode.max_delay();
            let buf = if self.already_timed_out || max_delay.is_none() {
                self.already_timed_out = false;
                receiver.recv().unwrap()
            } else {
                match receiver.recv_timeout(max_delay.unwrap()) {
                    Ok(buf) => buf,
                    Err(_) => {
                        self.already_timed_out = true;
                        vec![StreamElement::FlushBatch]
                    }
                }
            };
            self.buffer = buf.into();
        }
        let message = self
            .buffer
            .pop_front()
            .expect("Previous block sent an empty message");
        if matches!(message, StreamElement::End) {
            self.missing_ends -= 1;
            debug!(
                "{} received an end, {} more to come",
                metadata.coord, self.missing_ends
            );
            return self.next();
        }
        message
    }

    fn to_string(&self) -> String {
        format!("[{}]", std::any::type_name::<Out>())
    }
}

fn clone_none<T>(_: &Option<T>) -> Option<T> {
    None
}
