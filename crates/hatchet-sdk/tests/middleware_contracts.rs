use another_hatchet_sdk::*;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct Layer {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}
#[async_trait::async_trait]
impl TaskMiddleware for Layer {
    async fn handle(&self, invocation: TaskInvocation, next: Next) -> Result<Value> {
        self.events.lock().unwrap().push(format!(
            "{}:enter:{}",
            self.name,
            invocation.is_durable()
        ));
        let mut input = invocation.input().clone();
        input["path"].as_array_mut().unwrap().push(json!(self.name));
        let result = next.run(invocation.with_input(input)).await;
        self.events
            .lock()
            .unwrap()
            .push(format!("{}:exit", self.name));
        result
    }
}
fn ids(task: &str) -> ContextIds {
    ContextIds {
        workflow_name: "middleware".into(),
        task_name: task.into(),
        workflow_run_id: WorkflowRunId("middleware-run".into()),
        task_run_id: format!("{task}-run"),
        workflow_id: None,
        workflow_version_id: None,
    }
}

#[tokio::test]
async fn middleware_wraps_both_task_kinds_in_registration_order_and_can_transform_input() {
    let client = Client::new(Arc::new(InMemoryTransport::new()));
    let normal = client.task::<Value, Value>("normal");
    let durable = client.durable_task::<Value, Value>("durable");
    let events = Arc::new(Mutex::new(vec![]));
    let worker = client
        .worker("middleware-worker")
        .middleware(Layer {
            name: "outer",
            events: Arc::clone(&events),
        })
        .middleware(Layer {
            name: "inner",
            events: Arc::clone(&events),
        })
        .register_task(
            &normal,
            |input, _: InMemoryContext<InMemoryTransport>| async { Ok(input) },
        )
        .register_durable_task(
            &durable,
            |input, _: InMemoryDurableContext<InMemoryTransport>| async { Ok(input) },
        )
        .build()
        .start()
        .unwrap();
    assert_eq!(
        worker
            .run_task_json("normal", json!({"path":[]}), ids("normal"))
            .await
            .unwrap(),
        json!({"path":["outer","inner"]})
    );
    assert_eq!(
        worker
            .run_durable_task_json("durable", json!({"path":[]}), ids("durable"), 1)
            .await
            .unwrap(),
        json!({"path":["outer","inner"]})
    );
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            "outer:enter:false",
            "inner:enter:false",
            "inner:exit",
            "outer:exit",
            "outer:enter:true",
            "inner:enter:true",
            "inner:exit",
            "outer:exit"
        ]
    );
    worker.shutdown().await.unwrap();
}

#[derive(Debug)]
struct Reject;
#[async_trait::async_trait]
impl TaskMiddleware for Reject {
    async fn handle(&self, _: TaskInvocation, _: Next) -> Result<Value> {
        Err(Error::non_retryable("rejected by middleware"))
    }
}
#[tokio::test]
async fn middleware_can_reject_without_invoking_the_handler_and_preserves_error_kind() {
    let client = Client::new(Arc::new(InMemoryTransport::new()));
    let task = client.task::<Value, Value>("reject");
    let executions = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&executions);
    let worker = client
        .worker("reject-worker")
        .middleware(Reject)
        .register_task(&task, move |_, _: InMemoryContext<InMemoryTransport>| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!(true)) }
        })
        .build()
        .start()
        .unwrap();
    assert!(matches!(
        worker
            .run_task_json("reject", json!({}), ids("reject"))
            .await,
        Err(Error::NonRetryable(_))
    ));
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    worker.shutdown().await.unwrap();
}
