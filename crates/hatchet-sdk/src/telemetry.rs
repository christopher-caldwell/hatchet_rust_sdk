//! Native OpenTelemetry spans. Applications own their provider and exporters.
use crate::{Result, RunOpts, TaskInvocation};
use std::future::Future;

#[cfg(feature = "opentelemetry")]
mod enabled {
    use crate::Result;
    use opentelemetry::{
        Context, KeyValue,
        context::FutureExt,
        global,
        propagation::TextMapPropagator,
        trace::{SpanKind, Status, TraceContextExt, Tracer},
    };
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use std::collections::HashMap;

    pub(super) struct Scope {
        context: Context,
        finished: bool,
    }
    impl Scope {
        pub(super) fn new(
            name: &'static str,
            kind: SpanKind,
            parent: &Context,
            attributes: Vec<KeyValue>,
        ) -> Self {
            let tracer = global::tracer("another-hatchet-sdk");
            let span = tracer
                .span_builder(name)
                .with_kind(kind)
                .with_attributes(attributes)
                .start_with_context(&tracer, parent);
            Self {
                context: parent.with_span(span),
                finished: false,
            }
        }
        pub(super) fn context(&self) -> Context {
            self.context.clone()
        }
        pub(super) fn finish<R>(&mut self, result: &Result<R>) {
            match result {
                Ok(_) => self.context.span().set_status(Status::Ok),
                Err(error) => {
                    self.context.span().record_error(error);
                    self.context
                        .span()
                        .set_status(Status::error(error.to_string()));
                }
            }
            self.context.span().end();
            self.finished = true;
        }
    }
    impl Drop for Scope {
        fn drop(&mut self) {
            if !self.finished {
                self.context
                    .span()
                    .set_status(Status::error("execution future dropped before completion"));
                self.context.span().end();
            }
        }
    }
    pub(super) fn inject(context: &Context, metadata: &mut HashMap<String, String>) {
        TraceContextPropagator::new().inject_context(context, metadata);
    }
    pub(super) async fn run<F: std::future::Future>(future: F, context: Context) -> F::Output {
        future.with_context(context).await
    }
    pub(super) fn extract(metadata: &HashMap<String, String>) -> Context {
        TraceContextPropagator::new().extract_with_context(&Context::new(), metadata)
    }
}

pub(crate) async fn submission<R, F, Fut>(
    workflow_name: &str,
    mut opts: RunOpts,
    submit: F,
) -> Result<R>
where
    F: FnOnce(RunOpts) -> Fut,
    Fut: Future<Output = Result<R>>,
{
    #[cfg(feature = "opentelemetry")]
    {
        use opentelemetry::{Context, KeyValue, trace::SpanKind};
        let mut scope = enabled::Scope::new(
            "hatchet.run_workflow",
            SpanKind::Producer,
            &Context::current(),
            vec![KeyValue::new(
                "hatchet.workflow_name",
                workflow_name.to_owned(),
            )],
        );
        enabled::inject(&scope.context(), &mut opts.additional_metadata);
        let result = enabled::run(submit(opts), scope.context()).await;
        scope.finish(&result);
        result
    }
    #[cfg(not(feature = "opentelemetry"))]
    {
        let _ = workflow_name;
        let _ = &mut opts;
        submit(opts).await
    }
}

pub(crate) async fn task(
    invocation: TaskInvocation,
    next: crate::Next,
) -> Result<serde_json::Value> {
    #[cfg(feature = "opentelemetry")]
    {
        use opentelemetry::{KeyValue, trace::SpanKind};
        let mut scope = enabled::Scope::new(
            "hatchet.start_step_run",
            SpanKind::Consumer,
            &enabled::extract(invocation.metadata()),
            vec![
                KeyValue::new(
                    "hatchet.workflow_run_id",
                    invocation.ids().workflow_run_id.0.clone(),
                ),
                KeyValue::new("hatchet.step_run_id", invocation.ids().task_run_id.clone()),
                KeyValue::new("hatchet.task_name", invocation.task_name().to_owned()),
                KeyValue::new("hatchet.retry_count", i64::from(invocation.retry_count())),
                KeyValue::new("hatchet.durable", invocation.is_durable()),
            ],
        );
        let result = enabled::run(next.run(invocation), scope.context()).await;
        scope.finish(&result);
        result
    }
    #[cfg(not(feature = "opentelemetry"))]
    {
        next.run(invocation).await
    }
}

/// Adds the active span context to durable child requests as well as normal ones.
pub(crate) fn inject_current(metadata: &mut std::collections::HashMap<String, String>) {
    #[cfg(feature = "opentelemetry")]
    enabled::inject(&opentelemetry::Context::current(), metadata);
    #[cfg(not(feature = "opentelemetry"))]
    let _ = metadata;
}

#[cfg(all(test, feature = "opentelemetry"))]
mod tests {
    use super::*;
    use crate::{ContextIds, Error, Next, WorkflowRunId};
    use opentelemetry::{
        global,
        trace::{Status, TraceContextExt},
    };
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use serde_json::json;
    use std::{collections::HashMap, sync::Arc};
    fn invocation(name: &str, trace: u128) -> TaskInvocation {
        TaskInvocation::new(
            name.into(),
            ContextIds {
                workflow_name: "telemetry-test".into(),
                task_name: name.into(),
                workflow_run_id: WorkflowRunId(name.into()),
                task_run_id: format!("{name}-task"),
                workflow_id: None,
                workflow_version_id: None,
            },
            json!({"secret":"must not be recorded"}),
            HashMap::from([
                (
                    "traceparent".into(),
                    format!("00-{trace:032x}-0000000000000001-01"),
                ),
                ("tracestate".into(), "vendor=value".into()),
            ]),
            2,
            false,
        )
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spans_propagate_across_awaits_and_children_without_cross_task_leaks_and_end_on_abort()
    {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let first = tokio::spawn(task(
            invocation("trace-one", 11),
            Next::new(Arc::from([]), |_| {
                Box::pin(async {
                    let before = opentelemetry::Context::current()
                        .span()
                        .span_context()
                        .clone();
                    tokio::task::yield_now().await;
                    assert_eq!(
                        *opentelemetry::Context::current().span().span_context(),
                        before
                    );
                    submission("telemetry-child", RunOpts::default(), |opts| async move {
                        assert!(
                            opts.additional_metadata["traceparent"]
                                .contains("0000000000000000000000000000000b")
                        );
                        assert_eq!(opts.additional_metadata["tracestate"], "vendor=value");
                        Ok(json!(true))
                    })
                    .await
                })
            }),
        ));
        let second = tokio::spawn(task(
            invocation("trace-two", 22),
            Next::new(Arc::from([]), |_| {
                Box::pin(async {
                    tokio::task::yield_now().await;
                    assert_eq!(
                        opentelemetry::Context::current()
                            .span()
                            .span_context()
                            .trace_id()
                            .to_string(),
                        format!("{:032x}", 22)
                    );
                    Err(Error::non_retryable("expected failure"))
                })
            }),
        ));
        assert_eq!(first.await.unwrap().unwrap(), json!(true));
        assert!(second.await.unwrap().is_err());
        let entered = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&entered);
        let cancelled = tokio::spawn(task(
            invocation("trace-abort", 33),
            Next::new(Arc::from([]), move |_| {
                Box::pin(async move {
                    notify.notify_one();
                    std::future::pending().await
                })
            }),
        ));
        entered.notified().await;
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let named = |name: &str| {
            spans
                .iter()
                .find(|s| {
                    s.attributes
                        .iter()
                        .any(|a| a.key.as_str() == "hatchet.task_name" && a.value.as_str() == name)
                })
                .unwrap()
        };
        let one = named("trace-one");
        let two = named("trace-two");
        let abort = named("trace-abort");
        assert_eq!(one.parent_span_id.to_string(), "0000000000000001");
        assert_eq!(one.status, Status::Ok);
        assert!(matches!(two.status, Status::Error { .. }));
        assert!(matches!(abort.status, Status::Error { .. }));
        let child = spans
            .iter()
            .find(|s| {
                s.attributes.iter().any(|a| {
                    a.key.as_str() == "hatchet.workflow_name"
                        && a.value.as_str() == "telemetry-child"
                })
            })
            .unwrap();
        assert_eq!(child.parent_span_id, one.span_context.span_id());
        assert_eq!(child.span_context.trace_id(), one.span_context.trace_id());
        assert!(!format!("{spans:?}").contains("must not be recorded"));
        assert!(
            !opentelemetry::Context::current()
                .span()
                .span_context()
                .is_valid()
        );
        provider.shutdown().unwrap();
    }
}
