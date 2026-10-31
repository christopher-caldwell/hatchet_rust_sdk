//! Connection diagnostics for long-running workers.
//!
//! A worker that loses its engine must show which call it is waiting on. Lines carry their own
//! time, in seconds since the Unix epoch, because a platform's log timestamps can lag by most of an
//! hour while its host is failing. These helpers only observe: a watched call runs exactly as it
//! would without them, and they add no timeouts.

use std::fmt::Display;
use std::future::Future;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WAIT_REPORT_EVERY: Duration = Duration::from_secs(30);
const SLOW_CALL: Duration = Duration::from_secs(1);

pub(crate) fn log(message: impl Display) {
    eprintln!("{}", line(message));
}

fn line(message: impl Display) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "hatchet-sdk ts={}.{:03} {message}",
        now.as_secs(),
        now.subsec_millis()
    )
}

/// Awaits `call`, reporting while it stays pending and once when a slow call finishes.
pub(crate) async fn traced<F: Future>(call: &'static str, future: F) -> F::Output {
    watched(call, WAIT_REPORT_EVERY, SLOW_CALL, log, future).await
}

async fn watched<F: Future>(
    call: &'static str,
    report_every: Duration,
    slow: Duration,
    report: impl Fn(String),
    future: F,
) -> F::Output {
    let started = Instant::now();
    let mut waiting = std::pin::pin!(future);
    let mut reports =
        tokio::time::interval_at(tokio::time::Instant::now() + report_every, report_every);
    let output = loop {
        tokio::select! {
            output = &mut waiting => break output,
            _ = reports.tick() => {
                report(format!("still waiting on {call} after {}s", started.elapsed().as_secs()));
            }
        }
    };
    let elapsed = started.elapsed();
    if elapsed >= slow {
        report(format!("{call} finished after {}ms", elapsed.as_millis()));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    async fn reports_for(pending_for: Duration) -> (u32, Vec<String>) {
        let reports = Mutex::new(Vec::new());
        let output = watched(
            "test_call",
            Duration::from_millis(40),
            Duration::from_millis(100),
            |message| reports.lock().unwrap().push(message),
            async {
                tokio::time::sleep(pending_for).await;
                7
            },
        )
        .await;
        (output, reports.into_inner().unwrap())
    }

    #[tokio::test]
    async fn a_pending_call_is_reported_by_name_and_keeps_its_output() {
        let (output, reports) = reports_for(Duration::from_millis(150)).await;

        assert_eq!(output, 7);
        assert!(
            reports
                .iter()
                .any(|report| report.starts_with("still waiting on test_call after")),
            "{reports:?}"
        );
        assert!(
            reports
                .last()
                .is_some_and(|report| report.starts_with("test_call finished after")),
            "{reports:?}"
        );
    }

    #[tokio::test]
    async fn a_fast_call_is_not_reported() {
        let (output, reports) = reports_for(Duration::ZERO).await;

        assert_eq!(output, 7);
        assert!(reports.is_empty(), "{reports:?}");
    }

    #[test]
    fn a_line_carries_its_own_time() {
        let line = line("event");

        assert!(line.starts_with("hatchet-sdk ts="), "{line}");
        assert!(line.ends_with(" event"), "{line}");
    }
}
