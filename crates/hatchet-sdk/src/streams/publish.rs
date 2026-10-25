use super::*;
use std::{collections::VecDeque, sync::atomic::Ordering};
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;

pub(super) struct Topic {
    queue: Mutex<Queue>,
    notify: Notify,
    producer: Mutex<Producer>,
}
struct Queue {
    items: VecDeque<Pending>,
    running: bool,
}
struct Pending {
    payload: Vec<u8>,
    send_by: Instant,
    reply: oneshot::Sender<Result<()>>,
}
#[derive(Clone)]
struct Producer {
    id: String,
    seq: i64,
}
impl Default for Producer {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            seq: 0,
        }
    }
}
// An RPC may have stored the message even when its future is aborted. Never let
// the next payload reuse that producer/sequence after an ambiguous outcome.
struct Attempt<'a> {
    producer: &'a mut Producer,
    ambiguous: bool,
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if self.ambiguous {
            *self.producer = Producer::default();
        }
    }
}
fn rejected_before_storage(code: Code) -> bool {
    matches!(
        code,
        Code::InvalidArgument
            | Code::ResourceExhausted
            | Code::Unauthenticated
            | Code::PermissionDenied
            | Code::Unimplemented
    )
}
fn fitting(items: &VecDeque<Pending>) -> usize {
    let mut bytes = 0;
    let mut count = 0;
    for item in items.iter().take(500) {
        let next = if item.payload.len() > MAX_INLINE {
            256
        } else {
            item.payload.len()
        } + 16;
        if count > 0 && bytes + next > MAX_INLINE {
            break;
        }
        count += 1;
        bytes += next;
    }
    count
}
impl StreamsClient {
    /// Resolves only after storage is acknowledged. Same-topic calls are queued
    /// in poll/enqueue order, batched up to 500 entries / the wire byte limit.
    /// Dropping this future after enqueue does not retract the publish.
    /// # Errors
    /// Rejects empty/oversized payloads and out-of-range batching waits, or
    /// returns upload/engine errors without claiming an ambiguous write succeeded.
    pub async fn publish(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
        options: StreamOptions,
    ) -> Result<()> {
        validate_address(topic, &options.namespace)?;
        let payload = payload.into();
        if payload.is_empty() || payload.len() > MAX_UPLOADED {
            return Err(Error::InputValidation(vec![format!(
                "stream payload must contain 1..={MAX_UPLOADED} bytes"
            )]));
        }
        let send_by = Instant::now()
            .checked_add(options.batch_timeout)
            .ok_or_else(|| {
                Error::InputValidation(vec!["stream batch timeout exceeds the clock range".into()])
            })?;
        let key = (options.namespace, topic.to_owned());
        let topic_state = self
            .inner
            .topics
            .lock()
            .await
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(Topic {
                    queue: Mutex::new(Queue {
                        items: VecDeque::new(),
                        running: false,
                    }),
                    notify: Notify::new(),
                    producer: Mutex::new(Producer::default()),
                })
            })
            .clone();
        let (reply, receive) = oneshot::channel();
        let mut queue = topic_state.queue.lock().await;
        queue.items.push_back(Pending {
            payload,
            send_by,
            reply,
        });
        if !queue.running {
            queue.running = true;
            let client = self.clone();
            let state = Arc::clone(&topic_state);
            tokio::spawn(async move {
                client.drain(key, state).await;
            });
        }
        drop(queue);
        topic_state.notify.notify_one();
        receive.await.map_err(|_| {
            Error::Transport("stream publish queue closed before acknowledgment".into())
        })?
    }
    async fn next_batch(topic: &Topic) -> Option<Vec<Pending>> {
        loop {
            let mut queue = topic.queue.lock().await;
            if queue.items.is_empty() {
                queue.running = false;
                return None;
            }
            let count = fitting(&queue.items);
            let send_by = queue
                .items
                .iter()
                .map(|p| p.send_by)
                .min()
                .expect("nonempty queue");
            if count < queue.items.len() || count == 500 || send_by <= Instant::now() {
                return Some(queue.items.drain(..count).collect());
            }
            drop(queue);
            tokio::select! {_=topic.notify.notified()=>{},_=tokio::time::sleep_until(send_by)=>{}}
        }
    }
    async fn drain(&self, key: (String, String), topic: Arc<Topic>) {
        while let Some(batch) = Self::next_batch(&topic).await {
            let mut pending = Vec::new();
            let mut entries = Vec::new();
            for item in batch {
                let entry = if item.payload.len() > MAX_INLINE {
                    match self.upload_payload(&item.payload).await {
                        Ok(reference) => v1::StreamPublishEntry {
                            payload: Vec::new(),
                            payload_ref: reference,
                        },
                        Err(error) => {
                            let _ = item.reply.send(Err(error));
                            continue;
                        }
                    }
                } else {
                    v1::StreamPublishEntry {
                        payload: item.payload,
                        payload_ref: String::new(),
                    }
                };
                pending.push(item.reply);
                entries.push(entry);
            }
            if entries.is_empty() {
                continue;
            }
            if !self.inner.single_only.load(Ordering::Acquire) {
                match self.publish_ordered(&key, &topic, entries.clone()).await {
                    Ok(()) => {
                        for reply in pending {
                            let _ = reply.send(Ok(()));
                        }
                        continue;
                    }
                    Err(error) if error.code() == Code::Unimplemented => {
                        self.inner.single_only.store(true, Ordering::Release)
                    }
                    Err(error) => {
                        for reply in pending {
                            let _ = reply.send(Err(grpc_error(error.clone())));
                        }
                        continue;
                    }
                }
            }
            // UNIMPLEMENTED stored nothing. Older engines settle each fallback
            // publish individually; preserve partial successes and their order.
            for (entry, reply) in entries.into_iter().zip(pending) {
                let result = self
                    .publish_ordered(&key, &topic, vec![entry])
                    .await
                    .map_err(grpc_error);
                let _ = reply.send(result);
            }
        }
    }
    async fn publish_ordered(
        &self,
        key: &(String, String),
        topic: &Topic,
        entries: Vec<v1::StreamPublishEntry>,
    ) -> std::result::Result<(), Status> {
        let mut producer = topic.producer.lock().await;
        for attempt in 0..2 {
            if producer.seq > i64::MAX - entries.len() as i64 {
                *producer = Producer::default();
            }
            let mut guard = Attempt {
                producer: &mut producer,
                ambiguous: true,
            };
            let result = if self.inner.single_only.load(Ordering::Acquire) && entries.len() == 1 {
                let entry = &entries[0];
                let request = self.inner.request(v1::PublishStreamMessageRequest {
                    namespace: key.0.clone(),
                    topic: key.1.clone(),
                    producer_id: guard.producer.id.clone(),
                    producer_seq: guard.producer.seq,
                    payload: entry.payload.clone(),
                    payload_ref: entry.payload_ref.clone(),
                });
                tokio::time::timeout(RPC_TIMEOUT, self.inner.grpc.clone().publish(request))
                    .await
                    .map(|r| r.map(|_| ()))
            } else {
                let request = self.inner.request(v1::PublishStreamMessagesRequest {
                    namespace: key.0.clone(),
                    topic: key.1.clone(),
                    producer_id: guard.producer.id.clone(),
                    first_producer_seq: guard.producer.seq,
                    entries: entries.clone(),
                });
                tokio::time::timeout(RPC_TIMEOUT, self.inner.grpc.clone().publish_batch(request))
                    .await
                    .map(|r| r.map(|_| ()))
            }
            .unwrap_or_else(|_| Err(Status::deadline_exceeded("stream publish timed out")));
            match result {
                Ok(()) => {
                    guard.producer.seq += entries.len() as i64;
                    guard.ambiguous = false;
                    return Ok(());
                }
                Err(error) => {
                    if rejected_before_storage(error.code()) {
                        guard.ambiguous = false;
                    }
                    if error.code() == Code::FailedPrecondition && attempt == 0 {
                        drop(guard);
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        unreachable!("bounded producer-gap loop always returns")
    }
}
