use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coarsetime::Instant;
use parking_lot::Mutex;

use crate::network::{Coord, NetworkMessage, NetworkSender, NetworkTrySendError};
use crate::operator::StreamElement;

/// Which policy to use for batching the messages before sending them.
///
/// Avoid constructing directly this enumeration, please use [`BatchMode::fixed()`] and
/// [`BatchMode::adaptive()`] constructors.
///
/// The default batch mode is `Adaptive(1024, 50ms)`, meaning that a batch is flushed either when
/// it has at least 1024 messages, or no message has been received in the last 50ms.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BatchMode {
    /// A batch is flushed only when the specified number of messages is present.
    Fixed(NonZeroUsize),
    /// A batch is flushed only when the specified number of messages is present or a timeout
    /// expires. NOTE: The timer is checked only when a new message arrives, for background
    /// timers use BatchMode::Timed
    Adaptive(NonZeroUsize, Duration),
    /// A batch is flushed only when the specified number of messages is present or a timeout
    /// expires. The timer is checked in the background. the `tokio` feature is recommended for
    /// best performance when using this mode.
    Timed {
        max_size: NonZeroUsize,
        interval: Duration,
    },

    /// Send each message infdividually
    Single,
}

impl BatchMode {
    pub fn max_size(&self) -> usize {
        match self {
            BatchMode::Fixed(s) => s.get(),
            BatchMode::Adaptive(s, _) => s.get(),
            BatchMode::Timed { max_size, .. } => max_size.get(),
            BatchMode::Single => 1,
        }
    }

    pub fn interval(&self) -> Option<Duration> {
        match self {
            BatchMode::Adaptive(_, ts) => Some(*ts),
            BatchMode::Timed { interval, .. } => Some(*interval),
            _ => None,
        }
    }
}

/// A `Batcher` wraps a sender and sends the messages in batches to reduce the network overhead.
///
/// Internally it spawns a new task to handle the timeouts and join it at the end.
pub(crate) struct BatcherInner<Out: Send + 'static> {
    /// Sender used to communicate with the other replicas
    remote_sender: NetworkSender<Out>,
    /// Batching mode used by the batcher
    mode: BatchMode,
    /// Buffer used to keep messages ready to be sent
    buffer: Vec<StreamElement<Out>>,
    /// Time of the last flush of the buffer.    
    last_send: Instant,
    /// The coordinate of this block, used for marking the sender of the batch.
    coord: Coord,

    finished: bool,
}

impl<Out: Send + 'static> BatcherInner<Out> {
    /// Put a message in the batch queue, it won't be sent immediately.
    pub(crate) fn enqueue(&mut self, message: StreamElement<Out>) {
        match self.mode {
            BatchMode::Adaptive(n, max_delay) => {
                self.buffer.push(message);
                let timeout_elapsed = self.last_send.elapsed() > max_delay.into();
                if self.buffer.len() >= n.get() || timeout_elapsed {
                    self.flush()
                }
            }
            BatchMode::Fixed(n) => {
                self.buffer.push(message);
                if self.buffer.len() >= n.get() {
                    self.flush()
                }
            }
            BatchMode::Timed { max_size, .. } => {
                self.buffer.push(message);
                if self.buffer.len() >= max_size.get() {
                    self.flush()
                }
            }
            BatchMode::Single => {
                let message = NetworkMessage::new_single(message, self.coord);
                self.remote_sender.send(message).unwrap();
            }
        }
    }

    /// Flush the internal buffer if it's not empty.
    pub(crate) fn flush(&mut self) {
        if !self.buffer.is_empty() {
            let cap = self.buffer.capacity();
            let new_cap = if self.buffer.len() < cap / 4 {
                cap / 2
            } else {
                cap
            };
            let mut batch = Vec::with_capacity(new_cap);
            std::mem::swap(&mut self.buffer, &mut batch);
            let message = NetworkMessage::new_batch(batch, self.coord);
            self.remote_sender.send(message).unwrap();
            self.last_send = Instant::now();
        }
    }

    /// Flush the internal buffer if it's not empty.
    pub(crate) fn try_flush(&mut self) -> bool {
        if !self.buffer.is_empty() {
            let cap = self.buffer.capacity();
            let new_cap = if self.buffer.len() < cap / 4 {
                cap / 2
            } else {
                cap
            };
            let mut batch = Vec::with_capacity(new_cap);
            std::mem::swap(&mut self.buffer, &mut batch);
            let message = NetworkMessage::new_batch(batch, self.coord);
            match self.remote_sender.try_send(message) {
                Ok(_) => {
                    self.last_send = Instant::now();
                    true
                }
                Err(NetworkTrySendError::Full(m)) => {
                    self.buffer = m.into_vec();
                    false
                }
                Err(e) => {
                    panic!("failed sending message: {e}");
                }
            }
        } else {
            false
        }
    }

    /// Tell the batcher that the stream is ended, flush all the remaining messages.
    pub(crate) fn end(&mut self) {
        // Send the remaining messages
        if !self.buffer.is_empty() {
            let message = NetworkMessage::new_batch(std::mem::take(&mut self.buffer), self.coord);
            self.remote_sender.send(message).unwrap();
        }
        self.finished = true;
    }
}

pub(crate) enum Batcher<T: Send + 'static> {
    Unsync(BatcherInner<T>),
    Sync {
        inner: Arc<Mutex<BatcherInner<T>>>,
        cancel_token: Arc<AtomicBool>,
    },
}

impl<T: Clone + Send + 'static> Batcher<T> {
    pub(crate) fn new(remote_sender: NetworkSender<T>, mode: BatchMode, coord: Coord) -> Self {
        match mode {
            BatchMode::Timed { interval, .. } => {
                let inner = BatcherInner {
                    remote_sender,
                    mode,
                    buffer: Default::default(),
                    last_send: Instant::now(),
                    coord,
                    finished: false,
                };
                let inner = Arc::new(Mutex::new(inner));
                let batcher = inner.clone();
                let cancel_token = Arc::new(AtomicBool::new(false));
                let cancel = cancel_token.clone();
                #[cfg(feature = "tokio")]
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(interval);
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    interval.reset();
                    loop {
                        if cancel.load(Ordering::Acquire) {
                            break;
                        }
                        interval.tick().await;
                        batcher.lock().try_flush();
                    }
                });
                // TODO: This currently spawns A LOT of threads, we should switch to a time wheel version
                #[cfg(not(feature = "tokio"))]
                std::thread::spawn(move || {
                    loop {
                        std::thread::sleep(interval); // TODO: exact interval with time diff
                        if cancel.load(Ordering::Acquire) {
                            break;
                        }
                        batcher.lock().try_flush();
                    }
                });
                Self::Sync {
                    inner,
                    cancel_token,
                }
            }
            mode => Self::Unsync(BatcherInner {
                remote_sender,
                mode,
                buffer: Default::default(),
                last_send: Instant::now(),
                coord,
                finished: false,
            }),
        }
    }
    pub(crate) fn enqueue(&mut self, message: StreamElement<T>) {
        match self {
            Batcher::Unsync(inner) => inner.enqueue(message),
            Batcher::Sync { inner, .. } => inner.lock().enqueue(message),
        }
    }
    pub(crate) fn flush(&mut self) {
        match self {
            Batcher::Unsync(inner) => inner.flush(),
            Batcher::Sync { inner, .. } => inner.lock().flush(),
        }
    }
    pub(crate) fn end(self) {
        match self {
            Batcher::Unsync(mut inner) => inner.end(),
            Batcher::Sync {
                inner,
                cancel_token,
            } => {
                cancel_token.store(true, Ordering::Release);
                inner.lock().end()
            }
        }
    }
}

impl BatchMode {
    /// Construct a new `BatchMode::Fixed` with the given positive batch size.
    pub fn fixed(size: usize) -> BatchMode {
        BatchMode::Fixed(NonZeroUsize::new(size).expect("The batch size must be positive"))
    }

    /// Construct a new `BatchMode::Adaptive` with the given positive batch size and maximum delay.
    pub fn adaptive(size: usize, max_delay: Duration) -> BatchMode {
        BatchMode::Adaptive(
            NonZeroUsize::new(size).expect("The batch size must be positive"),
            max_delay,
        )
    }

    /// Construct a new `BatchMode::Adaptive` with the given positive batch size and maximum delay.
    pub fn timed(size: usize, interval: Duration) -> BatchMode {
        #[cfg(not(feature = "tokio"))]
        tracing::warn!("Currently, using timed batches without the tokio feature may spawn a large number of threads!");
        BatchMode::Timed {
            max_size: NonZeroUsize::new(size).expect("The batch size must be positive"),
            interval,
        }
    }

    /// Construct a new `BatchMode::Single`.
    pub fn single() -> BatchMode {
        BatchMode::Single
    }

    pub fn max_delay(&self) -> Option<Duration> {
        match &self {
            BatchMode::Adaptive(_, max_delay) => Some(*max_delay),
            BatchMode::Timed { interval, .. } => Some(*interval),
            BatchMode::Fixed(_) | BatchMode::Single => None,
        }
    }
}

impl Default for BatchMode {
    fn default() -> Self {
        BatchMode::adaptive(1024, Duration::from_millis(50))
    }
}
