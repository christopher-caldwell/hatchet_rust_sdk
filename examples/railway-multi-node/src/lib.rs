use std::collections::BTreeMap;
use std::time::Duration;

use another_hatchet_sdk::{Client, TaskConfig, Transport, WorkflowDeclaration};
use serde::{Deserialize, Serialize};

pub const WORKFLOW_NAME: &str = "rust-order-processing";
pub const PROCESS_ORDER: &str = "process-order";
pub const SEND_ORDER_RECEIPT: &str = "send-order-receipt";
pub const ORDERS_WORKER: &str = "rust-orders-worker";
pub const NOTIFICATIONS_WORKER: &str = "rust-notifications-worker";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderInput {
    pub order_id: String,
    pub email: String,
    pub total_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessedOrder {
    pub order_id: String,
    pub email: String,
    pub total_cents: u64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub order_id: String,
    pub recipient: String,
    pub subject: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Orders,
    Notifications,
}

impl Role {
    /// Parses the required worker role.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing or unsupported role.
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        match value {
            Some("orders") => Ok(Self::Orders),
            Some("notifications") => Ok(Self::Notifications),
            Some(other) => Err(format!(
                "invalid HATCHET_NODE '{other}'; expected orders or notifications"
            )),
            None => Err("HATCHET_NODE is required (orders or notifications)".to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitConfig {
    pub interval: Duration,
    pub timeout: Duration,
}

impl WaitConfig {
    /// Builds a finite, low-frequency observation configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when polling is faster than one second or the timeout
    /// is not greater than the interval.
    pub fn new(interval: Duration, timeout: Duration) -> Result<Self, String> {
        if interval < Duration::from_secs(1) {
            return Err("wait interval must be at least 1 second".to_owned());
        }
        if timeout <= interval {
            return Err("wait timeout must be greater than the interval".to_owned());
        }
        Ok(Self { interval, timeout })
    }
}

impl Default for WaitConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3),
            timeout: Duration::from_secs(180),
        }
    }
}

/// Parses a required non-zero health port.
///
/// # Errors
///
/// Returns an error when the value is missing, zero, or not a `u16`.
pub fn parse_port(value: Option<&str>) -> Result<u16, String> {
    let raw = value.ok_or_else(|| "PORT is required".to_owned())?;
    let port = raw
        .parse::<u16>()
        .map_err(|_| format!("PORT must be a valid non-zero u16, got '{raw}'"))?;
    if port == 0 {
        return Err("PORT must be non-zero".to_owned());
    }
    Ok(port)
}

#[must_use]
pub fn labels(service: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("service".to_owned(), service.to_owned())])
}

#[must_use]
pub fn declaration<T: Transport + 'static>(
    client: &Client<T>,
) -> WorkflowDeclaration<OrderInput, Receipt> {
    let process = client.task::<OrderInput, ProcessedOrder>(PROCESS_ORDER);
    let receipt = client.task::<OrderInput, Receipt>(SEND_ORDER_RECEIPT);
    WorkflowDeclaration::new(WORKFLOW_NAME)
        .step(&process, TaskConfig::default())
        .step_after(&receipt, &[&process], TaskConfig::default())
}

#[must_use]
pub fn process_order(input: OrderInput) -> ProcessedOrder {
    ProcessedOrder {
        order_id: input.order_id,
        email: input.email,
        total_cents: input.total_cents,
        status: "inventory-reserved".to_owned(),
    }
}

#[must_use]
pub fn make_receipt(order: &ProcessedOrder) -> Receipt {
    Receipt {
        order_id: order.order_id.clone(),
        recipient: order.email.clone(),
        subject: format!("Receipt for order {}", order.order_id),
    }
}

#[must_use]
pub fn redact_secrets(input: &str) -> String {
    const SECRET_KEYS: [&str; 5] = [
        "HATCHET_CLIENT_TOKEN",
        "ADMIN_PASSWORD",
        "SERVER_AUTH_COOKIE_SECRETS",
        "DATABASE_URL",
        "DATABASE_PASSWORD",
    ];
    input
        .lines()
        .map(|line| {
            if SECRET_KEYS.iter().any(|key| line.starts_with(key)) {
                line.split_once('=').map_or_else(
                    || "[REDACTED]".to_owned(),
                    |(key, _)| format!("{key}=[REDACTED]"),
                )
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use another_hatchet_sdk::{Client, InMemoryTransport};
    use std::sync::Arc;

    #[test]
    fn payload_is_preserved_from_order_to_receipt() {
        let order = OrderInput {
            order_id: "ord_123".to_owned(),
            email: "buyer@example.test".to_owned(),
            total_cents: 4_299,
        };
        let processed = process_order(order.clone());
        let receipt = make_receipt(&processed);
        assert_eq!(processed.order_id, order.order_id);
        assert_eq!(processed.email, order.email);
        assert_eq!(processed.total_cents, order.total_cents);
        assert_eq!(processed.status, "inventory-reserved");
        assert_eq!(receipt.order_id, order.order_id);
        assert_eq!(receipt.recipient, order.email);
    }

    #[test]
    fn names_and_dependency_edge_are_stable() {
        let client = Client::new(Arc::new(InMemoryTransport::new()));
        let workflow = declaration(&client);
        assert_eq!(workflow.name, WORKFLOW_NAME);
        assert_eq!(workflow.steps[0].task_name, PROCESS_ORDER);
        assert_eq!(workflow.steps[1].task_name, SEND_ORDER_RECEIPT);
        assert_eq!(workflow.steps[1].parents, vec![PROCESS_ORDER]);
    }

    #[test]
    fn role_and_port_validation_reject_bad_environment_values() {
        assert_eq!(Role::parse(Some("orders")), Ok(Role::Orders));
        assert_eq!(Role::parse(Some("notifications")), Ok(Role::Notifications));
        assert!(Role::parse(None).is_err());
        assert!(Role::parse(Some("both")).is_err());
        assert_eq!(parse_port(Some("8080")), Ok(8080));
        assert!(parse_port(None).is_err());
        assert!(parse_port(Some("0")).is_err());
        assert!(parse_port(Some("invalid")).is_err());
    }

    #[test]
    fn wait_configuration_is_low_frequency_and_finite() {
        let config = WaitConfig::default();
        assert_eq!(config.interval, Duration::from_secs(3));
        assert_eq!(config.timeout, Duration::from_secs(180));
        assert!(WaitConfig::new(Duration::from_millis(500), Duration::from_secs(2)).is_err());
        assert!(WaitConfig::new(Duration::from_secs(5), Duration::from_secs(5)).is_err());
    }

    #[test]
    fn secret_values_are_redacted() {
        let source = "HATCHET_CLIENT_TOKEN=secret\nDATABASE_URL=postgres://secret\nSAFE=value";
        let redacted = redact_secrets(source);
        assert!(!redacted.contains("postgres://secret"));
        assert!(!redacted.contains("TOKEN=secret"));
        assert!(redacted.contains("SAFE=value"));
    }
}
