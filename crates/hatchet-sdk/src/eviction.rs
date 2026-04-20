//! Eviction manager for durable task lifecycle management.
//!
//! Tracks whether durable tasks are actively computing or passively waiting
//! (sleep, waitFor, child run). When the engine requests eviction via a
//! `serverEvict` notice on the durable stream, the manager applies a grace
//! period before aborting active tasks.
//!
//! Mirrors the TS SDK's `eviction/eviction-manager.ts`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

/// Key for tracking task state.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ActionKey {
    pub task_external_id: String,
    pub invocation_count: i32,
}

/// The current state of a durable task invocation.
#[derive(Debug, Clone)]
pub enum TaskState {
    /// The task is actively computing (running user code).
    Active,
    /// The task is passively waiting for an external condition.
    Waiting { kind: String, resource_id: String },
}

/// Configuration for the eviction manager.
#[derive(Debug, Clone)]
pub struct EvictionConfig {
    /// Grace period for active tasks before forced eviction.
    pub active_grace_period: Duration,
    /// Immediate eviction for passively waiting tasks.
    pub passive_eviction: bool,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            active_grace_period: Duration::from_secs(5),
            passive_eviction: true,
        }
    }
}

/// Manages durable task eviction with grace periods.
pub struct EvictionManager {
    states: Arc<RwLock<HashMap<ActionKey, TaskState>>>,
    cancellation_tokens: Arc<Mutex<HashMap<ActionKey, CancellationToken>>>,
    config: EvictionConfig,
}

impl EvictionManager {
    /// Creates a new eviction manager with the given configuration.
    #[must_use]
    pub fn new(config: EvictionConfig) -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
            cancellation_tokens: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Registers a new durable task invocation as active.
    pub async fn register(&self, key: ActionKey) -> CancellationToken {
        let token = CancellationToken::new();
        self.states
            .write()
            .await
            .insert(key.clone(), TaskState::Active);
        self.cancellation_tokens
            .lock()
            .await
            .insert(key, token.clone());
        token
    }

    /// Marks a task as actively computing.
    pub async fn mark_active(&self, key: &ActionKey) {
        if let Some(state) = self.states.write().await.get_mut(key) {
            *state = TaskState::Active;
        }
    }

    /// Marks a task as passively waiting.
    pub async fn mark_waiting(
        &self,
        key: &ActionKey,
        kind: impl Into<String>,
        resource_id: impl Into<String>,
    ) {
        if let Some(state) = self.states.write().await.get_mut(key) {
            *state = TaskState::Waiting {
                kind: kind.into(),
                resource_id: resource_id.into(),
            };
        }
    }

    /// Removes a task from tracking (completed or evicted).
    pub async fn remove(&self, key: &ActionKey) {
        self.states.write().await.remove(key);
        self.cancellation_tokens.lock().await.remove(key);
    }

    /// Handles a server eviction notice.
    ///
    /// - If the task is passively waiting, cancels immediately.
    /// - If the task is actively computing, waits for the grace period before cancelling.
    pub async fn handle_eviction(&self, key: &ActionKey) {
        let state = self.states.read().await.get(key).cloned();

        match state {
            Some(TaskState::Waiting { .. }) if self.config.passive_eviction => {
                // Immediate eviction for passive tasks
                if let Some(token) = self.cancellation_tokens.lock().await.get(key) {
                    token.cancel();
                }
            }
            Some(TaskState::Active) => {
                // Grace period for active tasks
                let grace = self.config.active_grace_period;
                let tokens = Arc::clone(&self.cancellation_tokens);
                let key = key.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(grace).await;
                    if let Some(token) = tokens.lock().await.get(&key) {
                        token.cancel();
                    }
                });
            }
            _ => {
                // Task not tracked or already evicted
            }
        }
    }

    /// Returns the current state of a tracked task.
    pub async fn state(&self, key: &ActionKey) -> Option<TaskState> {
        self.states.read().await.get(key).cloned()
    }

    /// Returns the cancellation token for a tracked task.
    pub async fn cancellation_token(&self, key: &ActionKey) -> Option<CancellationToken> {
        self.cancellation_tokens.lock().await.get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_mark_states() {
        let mgr = EvictionManager::new(EvictionConfig::default());
        let key = ActionKey {
            task_external_id: "task-1".to_owned(),
            invocation_count: 1,
        };

        let _token = mgr.register(key.clone()).await;
        assert!(matches!(mgr.state(&key).await, Some(TaskState::Active)));

        mgr.mark_waiting(&key, "sleep", "sleep-10s").await;
        assert!(matches!(
            mgr.state(&key).await,
            Some(TaskState::Waiting { .. })
        ));

        mgr.mark_active(&key).await;
        assert!(matches!(mgr.state(&key).await, Some(TaskState::Active)));

        mgr.remove(&key).await;
        assert!(mgr.state(&key).await.is_none());
    }

    #[tokio::test]
    async fn passive_eviction_cancels_immediately() {
        let mgr = EvictionManager::new(EvictionConfig::default());
        let key = ActionKey {
            task_external_id: "task-2".to_owned(),
            invocation_count: 1,
        };

        let token = mgr.register(key.clone()).await;
        mgr.mark_waiting(&key, "event", "user:signup").await;
        mgr.handle_eviction(&key).await;

        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn active_eviction_uses_grace_period() {
        let mgr = EvictionManager::new(EvictionConfig {
            active_grace_period: Duration::from_millis(50),
            passive_eviction: true,
        });
        let key = ActionKey {
            task_external_id: "task-3".to_owned(),
            invocation_count: 1,
        };

        let token = mgr.register(key.clone()).await;
        mgr.handle_eviction(&key).await;

        // Should not be cancelled yet (grace period hasn't elapsed)
        assert!(!token.is_cancelled());

        // Wait for grace period
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(token.is_cancelled());
    }
}
