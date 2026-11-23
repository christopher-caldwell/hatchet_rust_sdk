//! `#[workflow]` must register tasks whose attribute is written with a path
//! (`#[another_hatchet_sdk::task]`, `#[::another_hatchet_sdk::durable_task]`),
//! not only the bare `#[task]` / `#[durable_task]` forms.
//!
//! Separate file because `#[workflow]` emits a file-level `register_all` and
//! `WORKFLOW_NAME`, which would collide with `feature_set_7_proc_macros.rs`.
use std::sync::Arc;

use another_hatchet_sdk::{Client, ContextIds, InMemoryTransport, WorkflowRunId};

#[another_hatchet_sdk::workflow(name = "qualified.workflow")]
mod qualified_workflow {
    #[another_hatchet_sdk::task]
    pub async fn qualified<T: another_hatchet_sdk::Transport>(
        input: u32,
        _ctx: another_hatchet_sdk::InMemoryContext<T>,
    ) -> another_hatchet_sdk::Result<u32> {
        Ok(input + 10)
    }

    #[::another_hatchet_sdk::task]
    pub async fn leading_colon<T: another_hatchet_sdk::Transport>(
        input: u32,
        _ctx: another_hatchet_sdk::InMemoryContext<T>,
    ) -> another_hatchet_sdk::Result<u32> {
        Ok(input + 20)
    }

    #[another_hatchet_sdk::durable_task]
    pub async fn qualified_durable<T: another_hatchet_sdk::Transport>(
        input: u32,
        _ctx: another_hatchet_sdk::InMemoryDurableContext<T>,
    ) -> another_hatchet_sdk::Result<u32> {
        Ok(input + 30)
    }
}

fn ids(task_name: &str) -> ContextIds {
    ContextIds {
        workflow_name: "qualified.workflow".to_owned(),
        task_name: task_name.to_owned(),
        workflow_run_id: WorkflowRunId("qualified-run-1".to_owned()),
        task_run_id: format!("task-run-{task_name}"),
        workflow_id: None,
        workflow_version_id: None,
    }
}

#[tokio::test]
async fn register_all_includes_path_qualified_task_attributes() {
    let client = Client::new(Arc::new(InMemoryTransport::new()));
    let handle = register_all(&client, client.worker("qualified-worker"))
        .build()
        .start()
        .expect("worker should start");

    let qualified = handle
        .run_task(
            &qualified_workflow::qualified_task_handle(&client),
            5_u32,
            ids("qualified"),
        )
        .await;
    let leading_colon = handle
        .run_task(
            &qualified_workflow::leading_colon_task_handle(&client),
            5_u32,
            ids("leading_colon"),
        )
        .await;
    let qualified_durable = handle
        .run_durable_task(
            &qualified_workflow::qualified_durable_durable_task_handle(&client),
            5_u32,
            ids("qualified_durable"),
            1,
        )
        .await;
    handle.shutdown().await.expect("shutdown should succeed");

    assert_eq!(WORKFLOW_NAME, "qualified.workflow");
    assert_eq!(
        qualified.expect("#[another_hatchet_sdk::task] must be registered by register_all"),
        15
    );
    assert_eq!(
        leading_colon.expect("#[::another_hatchet_sdk::task] must be registered by register_all"),
        25
    );
    assert_eq!(
        qualified_durable
            .expect("#[another_hatchet_sdk::durable_task] must be registered by register_all"),
        35
    );
}
