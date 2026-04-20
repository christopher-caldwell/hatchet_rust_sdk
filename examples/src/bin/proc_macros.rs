use std::sync::Arc;

use hatchet_sdk::{
    Client, ContextIds, DurableContext, InMemoryContext, InMemoryDurableContext, InMemoryTransport,
    WorkflowRunId, durable_task, task, workflow,
};

#[workflow(name = "email.workflow")]
mod email_workflow {
    use super::{DurableContext, InMemoryContext, InMemoryDurableContext, durable_task, task};

    #[task]
    pub async fn render<T: hatchet_sdk::Transport>(
        input: String,
        _ctx: InMemoryContext<T>,
    ) -> hatchet_sdk::Result<String> {
        Ok(format!("rendered:{input}"))
    }

    #[durable_task]
    pub async fn await_approval<T: hatchet_sdk::Transport>(
        input: String,
        ctx: InMemoryDurableContext<T>,
    ) -> hatchet_sdk::Result<String> {
        Ok(format!("{input}:attempts={}", ctx.invocation_count()))
    }
}

fn ids(task_name: &str) -> ContextIds {
    ContextIds {
        workflow_name: "email.workflow".to_owned(),
        task_name: task_name.to_owned(),
        workflow_run_id: WorkflowRunId("run-macro".to_owned()),
        task_run_id: format!("task-{task_name}"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let worker = register_all(&client, client.worker("macro-example")).build();
    let handle = worker.start()?;

    let rendered = handle
        .run_task(
            &email_workflow::render_task_handle(&client),
            "hello".to_owned(),
            ids("render"),
        )
        .await?;

    let approved = handle
        .run_durable_task(
            &email_workflow::await_approval_durable_task_handle(&client),
            "approval".to_owned(),
            ids("await_approval"),
            3,
        )
        .await?;

    handle.shutdown().await?;

    println!("proc_macros workflow={WORKFLOW_NAME} rendered={rendered} durable={approved}");

    Ok(())
}
