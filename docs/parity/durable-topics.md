# Durable topic streams (preview)

`client.streams()?` accesses independent durable topics. Unlike workflow output
streams, topics retain messages without a workflow, and readers resume after an
opaque cursor. The engine must expose `V1Streams` and enable the tenant's
`durableStreams` entitlement. The verified target is v0.114.0. Older services
receive an explicit unsupported-operation error; there is no in-memory emulator.
This feature matches the current TS preview; Go has no equivalent feature client.

```rust,no_run
use another_hatchet_sdk::*;
use std::time::Duration;

async fn example(client: Client<GrpcTransport>) -> Result<()> {
    let streams = client.streams()?;
    let options = StreamOptions {
        namespace: "orders".into(),
        batch_timeout: Duration::from_millis(10),
        ..Default::default()
    };
    streams.publish("updates", b"order accepted".to_vec(), options.clone()).await?;
    let metadata = streams.topic_metadata("updates", options.clone()).await?;
    assert!(metadata.message_count >= 1);
    let mut events = streams.events("updates", options)?;
    if let Some(event) = events.next().await {
        let event = event?;
        // Save event.cursor to resume immediately after this delivered message.
        let _ = (event.payload, event.cursor);
    }
    Ok(())
}
```

Topic namespaces default to the empty string, independently of the configured
workflow/event namespace. Topic and namespace preserve their literal text, up
to the engine's 255-character limit, without NUL. `StreamOptions.cursor` and
`cancel` apply only to subscriptions; `batch_timeout` applies only to publish.

A publish resolves after storage is acknowledged. Cloned clients share a queue
and producer state per namespace/topic. Calls enter the queue when their Rust
futures are polled; each request contains up to 500 entries and stays within the
wire byte limit. Once enqueued, dropping a publish future does not retract it.
Batching waits until a message's deadline or a full request. The queue serializes
publication while large payload uploads run before their message is dispatched.
Uploads within a batch are processed in order; overlapping large-file upload
throughput is not promised.

`PublishBatch` falling back with `UNIMPLEMENTED` stores nothing. The client then
uses individual `Publish` calls and retains each call's partial success/error.
Successful messages advance the producer sequence. Rejections known to precede
storage preserve it; ambiguous errors rotate the producer ID, and producer-gap
errors get one retry with a fresh producer. Ambiguous writes are not automatically
retried or reported as successful. Publication RPCs have a 30-second deadline.

Payloads must contain 1 byte to 64 MiB. Values above the inline gRPC threshold
(4 MiB minus 5 KiB) use authenticated multipart upload and a payload reference.
Readers fetch referenced bytes through the authenticated payload endpoint.
Transient HTTP failures have three bounded request attempts; permanent failures
are returned. Response bodies have size limits. The engine owns retention,
quotas, storage durability and payload expiration.

`events` starts at the oldest retained message, or immediately after its explicit
cursor. Disconnects resubscribe with backoff from the last delivered cursor.
Failed payload fetches do not advance that cursor or skip buffered entries.
Invalid/expired cursors, permission failures and other permanent errors yield
one error and end. A server idle hangup ends the subscription; ordinary EOF
reconnects. Dropping the stream or cancelling `StreamOptions.cancel` closes the
request and any reconnect wait, without a forwarding task or unbounded reader
queue.

`reset` changes the starting position of future cursorless readers to after the
latest message. It preserves retained history, existing readers and explicit
cursor reads. `topic_metadata` returns the actual retained count, latest cursor,
tenant, topic, namespace and timestamp.

References: [TS StreamsClient](https://github.com/hatchet-dev/hatchet/blob/af128ec1672adaf8e3cc4afd339ce5c2c6f08c35/sdks/typescript/src/v1/client/features/streams.ts),
[TS publish queue](https://github.com/hatchet-dev/hatchet/blob/af128ec1672adaf8e3cc4afd339ce5c2c6f08c35/sdks/typescript/src/util/stream-publish-batch.ts),
[official wire contract](https://github.com/hatchet-dev/hatchet/blob/aef20b00755da5b1e7e69197e7189848309afb20/api-contracts/v1/streams.proto).
