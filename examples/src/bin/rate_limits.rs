use std::sync::Arc;

use hatchet_sdk::{Client, GrpcTransport, RateLimitDuration, Result, config::HatchetConfig};

/// Demonstrates TS-style server-backed rate limit upsert.
/// Requires Hatchet connection env vars such as `HATCHET_CLIENT_TOKEN`.
#[tokio::main]
async fn main() -> Result<()> {
    let config = HatchetConfig::from_env()?;
    let transport = Arc::new(GrpcTransport::from_config(config).await?);
    let client = Client::new(transport);

    let rate_limit_key = std::env::var("HATCHET_RATE_LIMIT_KEY")
        .unwrap_or_else(|_| "rust-sdk-rate-limit-example".to_owned());

    let key = client
        .rate_limits()
        .upsert(rate_limit_key.clone(), 100, RateLimitDuration::Minute)
        .await?;
    assert_eq!(key, rate_limit_key);

    let limits = client.rate_limits().list().await?;
    assert_eq!(limits.get(&key).copied(), Some(100));

    println!("server-backed rate limit {key} upserted and listed successfully");
    Ok(())
}
