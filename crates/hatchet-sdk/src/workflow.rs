use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::{DurableTaskHandle, TaskHandle};
use crate::durable::Conditions;
use crate::task::{ConcurrencyConfig, TaskConfig};
use another_hatchet_core::Priority;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowDeclaration<I, O> {
    pub name: String,
    pub on_crons: Vec<String>,
    pub on_events: Vec<String>,
    pub concurrency: Option<ConcurrencyConfig>,
    pub default_priority: Option<Priority>,
    pub task_defaults: TaskConfig,
    pub sticky: Option<StickyStrategy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency: Option<crate::IdempotencyConfig>,
    pub steps: Vec<WorkflowStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<String>,
    #[serde(default, skip)]
    pub(crate) on_failure_is_durable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_success: Option<String>,
    #[serde(default, skip)]
    pub(crate) on_success_is_durable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Default filter expressions applied to all event triggers.
    /// Mirrors the TS `{ expression: string }[]` on `WorkflowDeclaration`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_filters: Vec<DefaultFilterExpression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
    io: PhantomData<fn(I) -> O>,
}

/// A default filter expression for workflow event triggers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DefaultFilterExpression {
    pub expression: String,
    #[serde(default)]
    pub scope: String,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StickyStrategy {
    Soft,
    Hard,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowStep {
    pub task_name: String,
    pub config: TaskConfig,
    pub parents: Vec<String>,
    #[serde(default)]
    pub is_durable: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wait_for: Vec<Conditions>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cancel_if: Vec<Conditions>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip_if: Vec<Conditions>,
}

impl<I, O> WorkflowDeclaration<I, O> {
    #[must_use]
    pub fn idempotency(mut self, config: crate::IdempotencyConfig) -> Self {
        self.idempotency = Some(config);
        self
    }

    fn push_step(
        &mut self,
        task_name: String,
        config: TaskConfig,
        parents: Vec<String>,
        step_config: StepConditions,
        is_durable: bool,
    ) {
        self.steps.push(WorkflowStep {
            task_name,
            config,
            parents,
            is_durable,
            wait_for: step_config.wait_for,
            cancel_if: step_config.cancel_if,
            skip_if: step_config.skip_if,
        });
    }

    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            on_crons: Vec::new(),
            on_events: Vec::new(),
            concurrency: None,
            default_priority: None,
            task_defaults: TaskConfig::default(),
            sticky: None,
            idempotency: None,
            steps: Vec::new(),
            on_failure: None,
            on_failure_is_durable: false,
            on_success: None,
            on_success_is_durable: false,
            description: None,
            version: None,
            default_filters: Vec::new(),
            input_schema: None,
            io: PhantomData,
        }
    }

    #[must_use]
    pub fn on_cron(mut self, cron: impl Into<String>) -> Self {
        self.on_crons.push(cron.into());
        self
    }

    #[must_use]
    pub fn on_event(mut self, event: impl Into<String>) -> Self {
        self.on_events.push(event.into());
        self
    }

    #[must_use]
    pub fn concurrency(mut self, concurrency: ConcurrencyConfig) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    #[must_use]
    pub fn default_priority(mut self, priority: Priority) -> Self {
        self.default_priority = Some(priority);
        self
    }

    #[must_use]
    pub fn task_defaults(mut self, config: TaskConfig) -> Self {
        self.task_defaults = config;
        self
    }

    #[must_use]
    pub fn sticky(mut self, sticky: StickyStrategy) -> Self {
        self.sticky = Some(sticky);
        self
    }

    /// Sets the failure handler task. This task is run when any step in the
    /// workflow fails after retries are exhausted.
    #[must_use]
    pub fn on_failure_task<SI, SO>(mut self, task: &TaskHandle<SI, SO>) -> Self {
        self.on_failure = Some(task.task_name().to_owned());
        self.on_failure_is_durable = false;
        self
    }

    /// Sets the failure handler durable task.
    #[must_use]
    pub fn on_failure_durable<SI, SO>(mut self, task: &DurableTaskHandle<SI, SO>) -> Self {
        self.on_failure = Some(task.task_name().to_owned());
        self.on_failure_is_durable = true;
        self
    }

    /// Sets the success handler task. This task is run after the workflow
    /// completes successfully.
    #[must_use]
    pub fn on_success_task<SI, SO>(mut self, task: &TaskHandle<SI, SO>) -> Self {
        self.on_success = Some(task.task_name().to_owned());
        self.on_success_is_durable = false;
        self
    }

    /// Sets the success handler durable task.
    #[must_use]
    pub fn on_success_durable<SI, SO>(mut self, task: &DurableTaskHandle<SI, SO>) -> Self {
        self.on_success = Some(task.task_name().to_owned());
        self.on_success_is_durable = true;
        self
    }

    /// Sets an optional human-readable description for the workflow.
    #[must_use]
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Sets an optional workflow version string.
    #[must_use]
    pub fn version(mut self, ver: impl Into<String>) -> Self {
        self.version = Some(ver.into());
        self
    }

    /// Adds a default filter expression for event triggers.
    #[must_use]
    pub fn default_filter(mut self, expression: impl Into<String>) -> Self {
        self.default_filters.push(DefaultFilterExpression {
            expression: expression.into(),
            scope: String::new(),
            payload: None,
        });
        self
    }

    /// Adds a default filter expression with scope and optional payload.
    #[must_use]
    pub fn default_filter_with_scope(
        mut self,
        expression: impl Into<String>,
        scope: impl Into<String>,
        payload: Option<serde_json::Value>,
    ) -> Self {
        self.default_filters.push(DefaultFilterExpression {
            expression: expression.into(),
            scope: scope.into(),
            payload,
        });
        self
    }

    /// Generates workflow input metadata for the backend/dashboard. Runtime task
    /// enforcement is configured separately on the task's TaskConfig.
    #[must_use]
    pub fn input_schema_for<S: schemars::JsonSchema>(self) -> Self {
        self.input_schema(crate::schema::generated::<S>())
    }

    /// Sets an optional workflow-level input schema.
    #[must_use]
    pub fn input_schema(mut self, schema: Value) -> Self {
        self.input_schema = Some(schema);
        self
    }

    #[must_use]
    pub fn step<SI, SO>(mut self, task: &TaskHandle<SI, SO>, config: TaskConfig) -> Self {
        self.push_step(
            task.task_name().to_owned(),
            config,
            Vec::new(),
            StepConditions::default(),
            false,
        );
        self
    }

    #[must_use]
    pub fn step_after<SI, SO, PI, PO>(
        mut self,
        task: &TaskHandle<SI, SO>,
        parents: &[&TaskHandle<PI, PO>],
        config: TaskConfig,
    ) -> Self {
        self.push_step(
            task.task_name().to_owned(),
            config,
            parents
                .iter()
                .map(|parent| parent.task_name().to_owned())
                .collect(),
            StepConditions::default(),
            false,
        );
        self
    }

    #[must_use]
    pub fn step_with<SI, SO>(
        mut self,
        task: &TaskHandle<SI, SO>,
        config: TaskConfig,
        step_config: StepConditions,
    ) -> Self {
        self.push_step(
            task.task_name().to_owned(),
            config,
            Vec::new(),
            step_config,
            false,
        );
        self
    }

    #[must_use]
    pub fn step_after_with<SI, SO, PI, PO>(
        mut self,
        task: &TaskHandle<SI, SO>,
        parents: &[&TaskHandle<PI, PO>],
        config: TaskConfig,
        step_config: StepConditions,
    ) -> Self {
        self.push_step(
            task.task_name().to_owned(),
            config,
            parents
                .iter()
                .map(|parent| parent.task_name().to_owned())
                .collect(),
            step_config,
            false,
        );
        self
    }

    #[must_use]
    pub fn durable_step<SI, SO>(
        mut self,
        task: &DurableTaskHandle<SI, SO>,
        config: TaskConfig,
    ) -> Self {
        self.push_step(
            task.task_name().to_owned(),
            config,
            Vec::new(),
            StepConditions::default(),
            true,
        );
        self
    }

    #[must_use]
    pub fn durable_step_with<SI, SO>(
        mut self,
        task: &DurableTaskHandle<SI, SO>,
        config: TaskConfig,
        step_config: StepConditions,
    ) -> Self {
        self.push_step(
            task.task_name().to_owned(),
            config,
            Vec::new(),
            step_config,
            true,
        );
        self
    }

    #[must_use]
    pub fn step_after_names(
        mut self,
        task_name: impl Into<String>,
        config: TaskConfig,
        parents: impl IntoIterator<Item = impl Into<String>>,
        is_durable: bool,
        step_config: StepConditions,
    ) -> Self {
        self.push_step(
            task_name.into(),
            config,
            parents.into_iter().map(Into::into).collect(),
            step_config,
            is_durable,
        );
        self
    }

    #[must_use]
    pub(crate) fn materialized(self) -> Self {
        let mut materialized = self;
        let Some(on_success) = materialized.on_success.clone() else {
            return materialized;
        };

        let leaves = workflow_leaf_names(&materialized.steps);
        if leaves.is_empty() {
            return materialized;
        }

        materialized.steps.push(WorkflowStep {
            task_name: on_success,
            config: materialized.task_defaults.clone(),
            parents: leaves,
            is_durable: materialized.on_success_is_durable,
            wait_for: Vec::new(),
            cancel_if: Vec::new(),
            skip_if: Vec::new(),
        });

        materialized
    }
}

pub(crate) fn workflow_leaf_names(steps: &[WorkflowStep]) -> Vec<String> {
    let mut non_leaves = std::collections::BTreeSet::new();
    for step in steps {
        for parent in &step.parents {
            non_leaves.insert(parent.clone());
        }
    }

    steps
        .iter()
        .filter(|step| !non_leaves.contains(&step.task_name))
        .map(|step| step.task_name.clone())
        .collect()
}

/// Builder for attaching conditions to a workflow step.
///
/// Mirrors the TS `waitFor`, `cancelIf`, and `skipIf` fields on
/// `CreateWorkflowTaskOpts`.
#[derive(Debug, Clone, Default)]
pub struct StepConditions {
    pub wait_for: Vec<Conditions>,
    pub cancel_if: Vec<Conditions>,
    pub skip_if: Vec<Conditions>,
}

impl StepConditions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn wait_for(mut self, condition: impl Into<Conditions>) -> Self {
        self.wait_for.push(condition.into());
        self
    }

    #[must_use]
    pub fn cancel_if(mut self, condition: impl Into<Conditions>) -> Self {
        self.cancel_if.push(condition.into());
        self
    }

    #[must_use]
    pub fn skip_if(mut self, condition: impl Into<Conditions>) -> Self {
        self.skip_if.push(condition.into());
        self
    }
}
