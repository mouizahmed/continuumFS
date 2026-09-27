//! Wall-clock time as Unix nanoseconds, and its RFC 3339 form for JSON objects.

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ns() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970");
    i64::try_from(d.as_nanos()).expect("the clock is before 2262")
}

/// `2026-09-26T12:00:00Z` (UTC, whole seconds).
pub fn rfc3339(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn formats_known_instants() {
        assert_eq!(super::rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            super::rfc3339(1_758_888_000_000_000_000),
            "2025-09-26T12:00:00Z"
        );
        assert_eq!(
            super::rfc3339(951_782_400_000_000_000),
            "2000-02-29T00:00:00Z"
        );
    }
}
