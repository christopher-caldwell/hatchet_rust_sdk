use std::sync::Arc;
use std::time::Duration;

use another_hatchet_sdk::{
    Client, Context, ContextExt, GrpcTransport, InMemoryContext, config::HatchetConfig,
};
use hatchet_multi_node::{
    NOTIFICATIONS_WORKER, ORDERS_WORKER, OrderInput, PROCESS_ORDER, ProcessedOrder, Role,
    SEND_ORDER_RECEIPT, declaration, labels, make_receipt, parse_port, process_order,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let role = Role::parse(std::env::var("HATCHET_NODE").ok().as_deref())?;
    let port = parse_port(std::env::var("PORT").ok().as_deref())?;
    let config = HatchetConfig::from_env()?;
    let transport = Arc::new(GrpcTransport::from_config(config).await?);
    let client = Client::new(transport);

    let handle = match role {
        Role::Orders => {
            let process = client.task::<OrderInput, ProcessedOrder>(PROCESS_ORDER);
            let worker = client
                .worker(ORDERS_WORKER)
                .slots(2)
                .durable_slots(2)
                .labels(labels("orders"))
                .health_port(port)
                .register_task(&process, |input, ctx: InMemoryContext<_>| async move {
                    let result = process_order(input);
                    println!(
                        "{}",
                        serde_json::json!({
                            "event": "task_completed",
                            "worker": ORDERS_WORKER,
                            "action": PROCESS_ORDER,
                            "order_id": result.order_id,
                            "run_id": ctx.workflow_run_id().0,
                            "status": result.status,
                        })
                    );
                    Ok(result)
                })
                .build();
            client.register_workflow(declaration(&client)).await?;
            worker.start()?
        }
        Role::Notifications => {
            let receipt =
                client.task::<OrderInput, hatchet_multi_node::Receipt>(SEND_ORDER_RECEIPT);
            client
                .worker(NOTIFICATIONS_WORKER)
                .slots(2)
                .durable_slots(2)
                .labels(labels("notifications"))
                .health_port(port)
                .register_task(&receipt, |_input, ctx: InMemoryContext<_>| async move {
                    let processed: ProcessedOrder = ctx.parent_output("process-order").await?;
                    let receipt = make_receipt(&processed);
                    println!(
                        "{}",
                        serde_json::json!({
                            "event": "task_completed",
                            "worker": NOTIFICATIONS_WORKER,
                            "action": SEND_ORDER_RECEIPT,
                            "order_id": receipt.order_id,
                            "run_id": ctx.workflow_run_id().0,
                            "recipient": receipt.recipient,
                        })
                    );
                    Ok(receipt)
                })
                .build()
                .start()?
        }
    };

    handle.wait_until_ready(Duration::from_secs(30)).await?;
    println!(
        "{}",
        serde_json::json!({"event":"worker_ready", "role": format!("{role:?}").to_lowercase(), "port": port})
    );
    shutdown_signal().await?;
    handle.shutdown().await?;
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
