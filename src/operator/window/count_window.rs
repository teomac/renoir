use std::collections::VecDeque;
use std::num::NonZeroUsize;

use crate::operator::window::{Window, WindowDescription, WindowGenerator};
use crate::operator::{Data, DataKey, StreamElement, Timestamp};
use crate::stream::KeyValue;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct CountWindow {
    size: NonZeroUsize,
    step: NonZeroUsize,
}

impl CountWindow {
    pub fn new(size: usize, step: usize) -> Self {
        Self {
            size: NonZeroUsize::new(size).expect("CountWindow size must be positive"),
            step: NonZeroUsize::new(step).expect("CountWindow size must be positive"),
        }
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for CountWindow {
    type Generator = CountWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        CountWindowGenerator::new(self.clone())
    }

    fn to_string(&self) -> String {
        format!(
            "CountWindow[size={}, step={}]",
            self.size.get(),
            self.step.get()
        )
    }
}

#[derive(Clone, Debug)]
pub struct CountWindowGenerator<Key: DataKey, Out: Data> {
    descr: CountWindow,
    buffer: VecDeque<Out>,
    timestamp_buffer: VecDeque<Timestamp>,
    received_end: bool,
    last_watermark: Option<Timestamp>,
    _key: PhantomData<Key>,
}

impl<Key: DataKey, Out: Data> CountWindowGenerator<Key, Out> {
    fn new(descr: CountWindow) -> Self {
        Self {
            descr,
            buffer: Default::default(),
            timestamp_buffer: Default::default(),
            received_end: false,
            last_watermark: None,
            _key: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data> WindowGenerator<Key, Out> for CountWindowGenerator<Key, Out> {
    fn add(&mut self, item: StreamElement<KeyValue<Key, Out>>) {
        match item {
            StreamElement::Item((_, item)) => self.buffer.push_back(item),
            StreamElement::Timestamped((_, item), ts) => {
                self.buffer.push_back(item);
                self.timestamp_buffer.push_back(ts);
            }
            StreamElement::Watermark(ts) => self.last_watermark = Some(ts),
            StreamElement::FlushBatch => unreachable!("Windows do not handle FlushBatch"),
            StreamElement::End => {
                self.received_end = true;
            }
        }
    }

    fn next_window(&mut self) -> Option<Window<Key, Out>> {
        if self.buffer.len() >= self.descr.size.get()
            || (self.received_end && !self.buffer.is_empty())
        {
            let size = self.descr.size.get().min(self.buffer.len());
            let timestamp_items = self.timestamp_buffer.iter().take(size).max().cloned();
            let timestamp = match &(timestamp_items, self.last_watermark) {
                (Some(ts), Some(w)) => {
                    // Make sure timestamp is correct with respect to watermarks
                    Some((*ts).max(*w + Timestamp::from_nanos(1)))
                }
                (Some(ts), _) => Some(*ts),
                _ => None,
            };

            Some(Window {
                size,
                gen: self,
                timestamp,
            })
        } else {
            None
        }
    }

    fn advance(&mut self) {
        for _ in 0..self.descr.step.get() {
            self.buffer.pop_front();
            self.timestamp_buffer.pop_front();
        }
    }

    fn buffer(&self) -> &VecDeque<Out> {
        &self.buffer
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::{CountWindow, StreamElement, WindowDescription, WindowGenerator};
    use std::time::Duration;

    #[test]
    fn count_window_watermark() {
        let descr = CountWindow::new(3, 2);
        let mut generator = descr.new_generator();

        generator.add(StreamElement::Timestamped((0, 1), Duration::from_secs(1)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped((0, 2), Duration::from_secs(2)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Watermark(Duration::from_secs(4)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::End);
        let window = generator.next_window().unwrap();
        assert_eq!(
            window.timestamp,
            Some(Duration::from_nanos(1) + Duration::from_secs(4))
        );
        assert_eq!(window.size, 2);
    }

    #[test]
    fn count_window_timestamp() {
        let descr = CountWindow::new(3, 2);
        let mut generator = descr.new_generator();

        generator.add(StreamElement::Timestamped((0, 1), Duration::from_secs(1)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped((0, 2), Duration::from_secs(2)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped((0, 3), Duration::from_secs(3)));
        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(Duration::from_secs(3)));
        assert_eq!(window.size, 3);
    }
}
