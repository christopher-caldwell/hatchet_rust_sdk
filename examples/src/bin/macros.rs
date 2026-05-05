use hatchet_sdk::{
    Client, Context, GrpcTransport, InMemoryContext, InMemoryDurableContext, Result,
    config::HatchetConfig, durable_task, task, workflow,
};
use std::sync::Arc;

// 1. The `#[workflow]` macro cleanly namespaces the tasks within a specific module
// reducing boilerplate drastically for step declaration.
#[workflow(name = "user.signup")]
mod user_signup_workflow {
    use super::{Context, InMemoryContext, InMemoryDurableContext, durable_task, task};

    // 2. The `#[task]` macro automatically implements the `ErasedTaskFn` mapping to the handler
    #[task]
    pub async fn validate_data<T: hatchet_sdk::Transport>(
        input: serde_json::Value,
        ctx: InMemoryContext<T>,
    ) -> hatchet_sdk::Result<bool> {
        println!("[{}] Validating data...", ctx.task_name());
        Ok(input.get("email").is_some())
    }

    // 3. The `#[durable_task]` macro allows access to the expansive feature-set of DurableContext
    #[durable_task]
    pub async fn notify_admin<T: hatchet_sdk::Transport>(
        input: serde_json::Value,
        ctx: InMemoryDurableContext<T>,
    ) -> hatchet_sdk::Result<String> {
        println!(
            "[{}] Sent admin notification for {:?}",
            ctx.task_name(),
            input
        );
        Ok("Notified".to_owned())
    }
}

/// This example demonstrates how to use the macros to drastically simplify the
/// codebase, building entire workflow orchestrations ergonomically.
#[tokio::main]
async fn main() -> Result<()> {
    let config = HatchetConfig::from_env()?;
    let transport = Arc::new(GrpcTransport::from_config(config).await?);
    let client = Client::new(transport);

    // 4. Hook up the generated properties onto the builder utilizing `register_all` helper
    let builder = client.worker("macro-worker");
    let worker = register_all(&client, builder).build();
    let _handle = worker.start()?;

    println!(
        "Worker configured. To run macro-based flows natively, refer to tests/feature_set_7_proc_macros"
    );

    Ok(())
}
