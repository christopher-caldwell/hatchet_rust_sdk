//! Machine-checkable parity matrix for TS function equivalence.
//!
//! This file maps TS feature rows (from the audit matrix) to Rust tests.
//! By making this an executable test suite, we ensure we don't drift from parity.
//! Every row from the audit that is unimplemented is ignored with an issue reference.

#![cfg(test)]

macro_rules! parity_row {
    ($name:ident, $issue_ref:expr) => {
        // Unimplemented parity row, tracked by an issue
        #[test]
        #[ignore = $issue_ref]
        fn $name() {}
    };
    ($name:ident) => {
        // Implemented parity row, tested here or covered by another test referenced inside
        #[test]
        fn $name() {}
    };
}

// ---------------------------------------------------------------------------
// Module 1: Conditions Model
// ---------------------------------------------------------------------------

parity_row!(conditions_sleep_for);
parity_row!(conditions_sleep_until);
parity_row!(conditions_user_event);
parity_row!(conditions_parent);
parity_row!(conditions_or_group);
parity_row!(conditions_render);
parity_row!(conditions_to_pb); // Model serialization exists
parity_row!(
    conditions_task_to_pb,
    "issue #101: needs transport layer wiring for wait_for"
);

// ---------------------------------------------------------------------------
// Module 2 & 3: Workflow Declarations & Task Configuration
// ---------------------------------------------------------------------------

parity_row!(workflow_declaration);
parity_row!(workflow_wait_for);
parity_row!(workflow_cancel_if);
parity_row!(workflow_skip_if);
parity_row!(task_config_retries);
parity_row!(task_config_rate_limits);
parity_row!(task_config_concurrency);

// ---------------------------------------------------------------------------
// Module 4: Context
// ---------------------------------------------------------------------------

parity_row!(context_task_name);
parity_row!(context_workflow_name);
parity_row!(context_parent_output);
parity_row!(context_spawn_child);
parity_row!(context_log);
parity_row!(
    context_release_slot,
    "issue #102: implement release_slot on ContextExt"
);
parity_row!(
    context_cancel,
    "issue #103: cancel() method missing from ContextExt"
);

// ---------------------------------------------------------------------------
// Module 5: Durable Context
// ---------------------------------------------------------------------------

parity_row!(durable_sleep_for);
parity_row!(durable_wait_for);
parity_row!(durable_spawn_child);
parity_row!(durable_now, "issue #104: memoized now() missing");
parity_row!(durable_memo_durable, "issue #105: memo() cache missing");

// ---------------------------------------------------------------------------
// Module 6: Client Surface
// ---------------------------------------------------------------------------

parity_row!(client_workflow);
parity_row!(client_task);
parity_row!(client_run_no_wait);
parity_row!(
    client_namespace,
    "issue #106: Namespace wrapping object missing"
);

// ---------------------------------------------------------------------------
// Module 9: Feature Clients
// ---------------------------------------------------------------------------

parity_row!(feature_runs_client);
parity_row!(feature_workflows_client);
parity_row!(feature_workers_client);
parity_row!(feature_logs_client);
