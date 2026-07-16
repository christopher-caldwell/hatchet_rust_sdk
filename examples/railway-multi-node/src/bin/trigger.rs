use std::sync::Arc;
use std::time::{Duration, Instant};

use hatchet_railway_multi_node::{OrderInput, Receipt, WORKFLOW_NAME, WaitConfig};
use another_hatchet_sdk::{Client, GrpcTransport, RunOpts, config::HatchetConfig, proto::v1::RunStatus};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(std::env::args().skip(1))?;
    eprintln!("trigger_stage=config");
    let config = HatchetConfig::from_env()?;
    eprintln!("trigger_stage=transport");
    let transport = Arc::new(GrpcTransport::from_config(config).await?);
    let client = Client::new(Arc::clone(&transport));
    eprintln!("trigger_stage=submit");
    let run = client
        .workflow::<OrderInput, Receipt>(WORKFLOW_NAME)
        .run_no_wait(args.order, RunOpts::default())
        .await?;
    eprintln!("trigger_stage=submitted");
    let run_id = run.run_id().0.clone();
    println!(
        "{}",
        serde_json::json!({"event":"run_submitted", "run_id":run_id})
    );
    if !args.wait {
        return Ok(());
    }

    let started = Instant::now();
    loop {
        if started.elapsed() >= args.wait_config.timeout {
            return Err(format!("timed out waiting for run {run_id}").into());
        }
        let details = transport.get_run_details(&run_id).await?;
        let status = RunStatus::try_from(details.status).ok();
        println!(
            "{}",
            serde_json::json!({"event":"run_observed", "run_id":run_id, "status":format!("{status:?}")})
        );
        if details.done {
            match status {
                Some(RunStatus::Completed) => {
                    let task = details
                        .task_runs
                        .values()
                        .find(|task| task.readable_id.ends_with("send-order-receipt"))
                        .ok_or("completed run lacks send-order-receipt output")?;
                    let bytes = task.output.as_ref().ok_or("receipt output is absent")?;
                    let receipt: Receipt = serde_json::from_slice(bytes)?;
                    println!(
                        "{}",
                        serde_json::json!({"event":"run_completed", "run_id":run_id, "receipt":receipt, "elapsed_ms":started.elapsed().as_millis()})
                    );
                    return Ok(());
                }
                Some(RunStatus::Failed) => {
                    return Err(format!("run {run_id} failed").into());
                }
                Some(RunStatus::Cancelled) => {
                    return Err(format!("run {run_id} was cancelled").into());
                }
                _ => return Err(format!("run {run_id} ended in unknown state {status:?}").into()),
            }
        }
        tokio::time::sleep(args.wait_config.interval).await;
    }
}

struct Args {
    order: OrderInput,
    wait: bool,
    wait_config: WaitConfig,
}

impl Args {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut order_id = "ord_demo_001".to_owned();
        let mut email = "buyer@example.test".to_owned();
        let mut total_cents = 4_299;
        let mut wait = false;
        let mut interval = Duration::from_secs(3);
        let mut timeout = Duration::from_secs(180);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--wait" => wait = true,
                "--order-id" => order_id = args.next().ok_or("--order-id needs a value")?,
                "--email" => email = args.next().ok_or("--email needs a value")?,
                "--total-cents" => {
                    total_cents = args
                        .next()
                        .ok_or("--total-cents needs a value")?
                        .parse()
                        .map_err(|_| "--total-cents must be an unsigned integer")?;
                }
                "--interval-seconds" => {
                    interval = Duration::from_secs(parse_seconds(&mut args, &arg)?);
                }
                "--timeout-seconds" => {
                    timeout = Duration::from_secs(parse_seconds(&mut args, &arg)?);
                }
                other => return Err(format!("unknown argument '{other}'")),
            }
        }
        Ok(Self {
            order: OrderInput {
                order_id,
                email,
                total_cents,
            },
            wait,
            wait_config: WaitConfig::new(interval, timeout)?,
        })
    }
}

fn parse_seconds(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<u64, String> {
    args.next()
        .ok_or_else(|| format!("{flag} needs a value"))?
        .parse()
        .map_err(|_| format!("{flag} must be an unsigned integer"))
}
