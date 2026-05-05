use std::sync::Arc;

use hatchet_sdk::{Client, InMemoryTransport, RateLimitDuration, Result};

/// Demonstrates the same rate-limit client API with `InMemoryTransport`.
/// This is useful for local/offline checks because it does not require Hatchet
/// connection environment variables.
#[tokio::main]
async fn main() -> Result<()> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport);

    let key = client
        .rate_limits()
        .upsert("api-burst", 100, RateLimitDuration::Minute)
        .await?;
    assert_eq!(key, "api-burst");

    let limits = client.rate_limits().list().await?;
    assert_eq!(limits.get("api-burst").copied(), Some(100));

    client.rate_limits().put("compat-local", 5).await;
    assert_eq!(
        client
            .rate_limits()
            .list()
            .await?
            .get("compat-local")
            .copied(),
        Some(5)
    );

    client.rate_limits().delete("compat-local").await;
    assert!(
        !client
            .rate_limits()
            .list()
            .await?
            .contains_key("compat-local")
    );

    println!("in-memory rate limit {key} upserted and listed successfully");
    Ok(())
}
