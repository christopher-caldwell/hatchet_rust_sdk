use crate::{Error, Result};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitPauseResult {
    pub key: String,
    pub paused_until: SystemTime,
}

pub(crate) fn timestamp(time: SystemTime) -> Result<prost_types::Timestamp> {
    let (seconds, nanos) = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => (
            i64::try_from(duration.as_secs()).ok(),
            duration.subsec_nanos() as i32,
        ),
        Err(error) => {
            let duration = error.duration();
            let seconds = i64::try_from(duration.as_secs())
                .ok()
                .and_then(i64::checked_neg);
            if duration.subsec_nanos() == 0 {
                (seconds, 0)
            } else {
                (
                    seconds.and_then(|s| s.checked_sub(1)),
                    (1_000_000_000 - duration.subsec_nanos()) as i32,
                )
            }
        }
    };
    let seconds = seconds
        .filter(|s| (-62_135_596_800..=253_402_300_799).contains(s))
        .ok_or_else(|| {
            Error::InputValidation(vec![
                "pause time is outside protobuf's year 1 through 9999 range".into(),
            ])
        })?;
    Ok(prost_types::Timestamp { seconds, nanos })
}
pub(crate) fn system_time(time: prost_types::Timestamp) -> Result<SystemTime> {
    if !(-62_135_596_800..=253_402_300_799).contains(&time.seconds)
        || !(0..1_000_000_000).contains(&time.nanos)
    {
        return Err(Error::Transport(
            "engine returned an invalid pause timestamp".into(),
        ));
    }
    let base = if time.seconds >= 0 {
        UNIX_EPOCH.checked_add(Duration::from_secs(time.seconds as u64))
    } else {
        UNIX_EPOCH.checked_sub(Duration::from_secs(time.seconds.unsigned_abs()))
    };
    base.and_then(|time_base| time_base.checked_add(Duration::from_nanos(time.nanos as u64)))
        .ok_or_else(|| {
            Error::Transport("engine pause timestamp cannot be represented on this platform".into())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn protobuf_pause_times_preserve_subseconds_and_reject_out_of_range_values() {
        for time in [
            UNIX_EPOCH - Duration::from_nanos(1),
            UNIX_EPOCH,
            UNIX_EPOCH + Duration::new(2_000_000_000, 123_456_789),
        ] {
            assert_eq!(system_time(timestamp(time).unwrap()).unwrap(), time);
        }
        assert!(
            system_time(prost_types::Timestamp {
                seconds: 253_402_300_800,
                nanos: 0
            })
            .is_err()
        );
        assert!(
            system_time(prost_types::Timestamp {
                seconds: 0,
                nanos: 1_000_000_000
            })
            .is_err()
        );
        if let Some(time) = UNIX_EPOCH.checked_add(Duration::from_secs(253_402_300_800)) {
            assert!(timestamp(time).is_err());
        }
    }
}
