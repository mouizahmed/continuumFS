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

/// The inverse of [`rfc3339`]: `None` unless `s` is exactly `YYYY-MM-DDTHH:MM:SSZ`.
pub fn parse_rfc3339(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() != 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'Z'
    {
        return None;
    }
    let num = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (year, month, day) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, m, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || h > 23 || m > 59 || sec > 59 {
        return None;
    }
    // Days since 1970-01-01 from a civil date (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some((days * 86_400 + h * 3600 + m * 60 + sec) * 1_000_000_000)
}

#[cfg(test)]
mod tests {
    #[test]
    fn parses_what_it_formats() {
        for ns in [
            0,
            1_758_888_000_000_000_000,
            951_782_400_000_000_000,
            4_102_444_799_000_000_000,
        ] {
            assert_eq!(super::parse_rfc3339(&super::rfc3339(ns)), Some(ns));
        }
        assert_eq!(super::parse_rfc3339("2026-13-01T00:00:00Z"), None);
        assert_eq!(super::parse_rfc3339("2026-09-26 12:00:00"), None);
    }

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
