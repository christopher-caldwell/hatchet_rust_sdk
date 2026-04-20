use std::sync::Arc;

use hatchet_sdk::{
    Client, ContextIds, DurableContext, InMemoryContext, InMemoryDurableContext, InMemoryTransport,
    WorkflowRunId, durable_task, task, workflow,
};

#[workflow(name = "macro.workflow")]
mod macro_workflow {
    use super::{DurableContext, InMemoryContext, InMemoryDurableContext, durable_task, task};

    #[task]
    pub async fn render<T: hatchet_sdk::Transport>(
        input: u32,
        _ctx: InMemoryContext<T>,
    ) -> hatchet_sdk::Result<u32> {
        Ok(input + 10)
    }

    #[durable_task]
    pub async fn await_approval<T: hatchet_sdk::Transport>(
        input: u32,
        ctx: InMemoryDurableContext<T>,
    ) -> hatchet_sdk::Result<u32> {
        Ok(input + ctx.invocation_count())
    }
}

fn ids(task_name: &str) -> ContextIds {
    ContextIds {
        workflow_name: "macro.workflow".to_owned(),
        task_name: task_name.to_owned(),
        workflow_run_id: WorkflowRunId("macro-run-1".to_owned()),
        task_run_id: format!("task-run-{task_name}"),
    }
}

#[tokio::test]
async fn validates_proc_macro_task_and_registration_sugar() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let builder = client.worker("macro-worker");
    let worker = register_all(&client, builder).build();
    let handle = worker.start().expect("worker should start");

    let render = handle
        .run_task(
            &macro_workflow::render_task_handle(&client),
            5_u32,
            ids("render"),
        )
        .await
        .expect("render task should run");
    assert_eq!(render, 15);

    let durable = handle
        .run_durable_task(
            &macro_workflow::await_approval_durable_task_handle(&client),
            20_u32,
            ids("await_approval"),
            2,
        )
        .await
        .expect("durable task should run");
    assert_eq!(durable, 22);

    assert_eq!(WORKFLOW_NAME, "macro.workflow");
    handle.shutdown().await.expect("shutdown should succeed");
}
