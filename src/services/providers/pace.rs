//! The rate gate an explorer's requests pass (the owner's word of
//! 2026-10-22, bsv-wallet-cli#50): each chain scanner has its own rate
//! limits, so when one fails the ladder goes to the next.
//!
//! A provider never sleeps on a rate limit. A 429 is a fault returned at
//! once ([`Error::RateLimited`], carrying the Retry-After seconds) and the
//! rung is closed until Retry-After passes: a call inside that window is
//! the same fault without a request. A provider with a published request
//! budget (BananaBlocks: 60 a minute, no key) also paces itself with a
//! token bucket, and a call with no token left is that fault too.
//!
//! A gate is the process's, not a `Services`'s: [`RateGate::shared`] hands
//! every provider built for one host (one base URL, one key) the same gate,
//! so a process that builds several `Services` (the daemon builds two per
//! tenant, `serve-fleet` one pair per tenant) spends one budget per host,
//! and a 429 one of them met closes the rung for all of them.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::StatusCode;

use crate::{Error, Result};

/// The longest a Retry-After closes a rung: a larger or garbled value from
/// the host is held to this.
pub const MAX_RETRY_AFTER_SECS: u64 = 600;

/// The process's gates, one per host key (see [`RateGate::shared`]).
static GATES: LazyLock<Mutex<HashMap<String, Arc<RateGate>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A token bucket: `capacity` requests at once, refilled at `per_sec`.
#[derive(Debug)]
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    /// A bucket that lets at most `per_minute` requests through in any 60 s
    /// window: half the budget as the burst, the other half refilled over
    /// the minute (burst + refill = the budget).
    fn per_minute(per_minute: u32, now: Instant) -> Self {
        let budget = f64::from(per_minute.max(2));
        let capacity = (budget / 2.0).floor();
        Self {
            capacity,
            tokens: capacity,
            per_sec: (budget - capacity) / 60.0,
            last: now,
        }
    }

    /// Take one token, or say how long until one is there.
    fn take(&mut self, now: Instant) -> std::result::Result<(), Duration> {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.per_sec).min(self.capacity);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - self.tokens) / self.per_sec))
        }
    }
}

/// One provider's gate: the Retry-After window and the optional pace.
#[derive(Debug)]
pub struct RateGate {
    provider: &'static str,
    default_retry_after: Duration,
    blocked_until: Mutex<Option<Instant>>,
    bucket: Option<Mutex<TokenBucket>>,
}

impl RateGate {
    /// A gate with no pace of its own; a 429 without a Retry-After closes
    /// the rung for `default_retry_after`.
    pub fn new(provider: &'static str, default_retry_after: Duration) -> Self {
        Self {
            provider,
            default_retry_after,
            blocked_until: Mutex::new(None),
            bucket: None,
        }
    }

    /// The process's gate for `key` (a provider's base URL, and its key
    /// when the budget is a key's), made on first use with `make`. Every
    /// provider built for the same key in this process shares it: one token
    /// bucket and one Retry-After window, however many `Services` hold it.
    pub fn shared(key: &str, make: impl FnOnce() -> RateGate) -> Arc<RateGate> {
        Arc::clone(
            lock(&GATES)
                .entry(key.to_string())
                .or_insert_with(|| Arc::new(make())),
        )
    }

    /// Pace the provider at most `per_minute` requests in any minute.
    pub fn with_per_minute(mut self, per_minute: u32) -> Self {
        self.bucket = Some(Mutex::new(TokenBucket::per_minute(
            per_minute,
            Instant::now(),
        )));
        self
    }

    /// Before a request: inside a Retry-After window, or with no token
    /// left, the call is [`Error::RateLimited`] and no request is made.
    pub fn admit(&self) -> Result<()> {
        self.admit_at(Instant::now())
    }

    fn admit_at(&self, now: Instant) -> Result<()> {
        if let Some(until) = *lock(&self.blocked_until) {
            if now < until {
                return Err(self.limited(until - now));
            }
        }
        if let Some(bucket) = &self.bucket {
            if let Err(wait) = lock(bucket).take(now) {
                return Err(self.limited(wait));
            }
        }
        Ok(())
    }

    /// After a response: a 429 closes the rung for its Retry-After (the
    /// default when the host sent none) and is the fault returned.
    pub fn check(&self, status: StatusCode, headers: &HeaderMap) -> Result<()> {
        if status != StatusCode::TOO_MANY_REQUESTS {
            return Ok(());
        }
        Err(self.close(headers))
    }

    /// A 429 came back: close the rung for its Retry-After (the default when
    /// the host sent none) and give the fault.
    pub fn close(&self, headers: &HeaderMap) -> Error {
        let secs = retry_after_secs(headers)
            .unwrap_or(self.default_retry_after.as_secs())
            .clamp(1, MAX_RETRY_AFTER_SECS);
        *lock(&self.blocked_until) = Some(Instant::now() + Duration::from_secs(secs));
        tracing::info!(
            provider = self.provider,
            retry_after_secs = secs,
            "rate limited (429): the rung is skipped until Retry-After passes"
        );
        Error::RateLimited {
            provider: self.provider.to_string(),
            retry_after_secs: secs,
        }
    }

    fn limited(&self, wait: Duration) -> Error {
        Error::RateLimited {
            provider: self.provider.to_string(),
            retry_after_secs: (wait.as_secs_f64().ceil() as u64).max(1),
        }
    }
}

/// The seconds a `Retry-After` header asks for: delta-seconds or an
/// HTTP-date (RFC 9110 section 10.2.3). `None` when absent or garbled.
pub fn retry_after_secs(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(secs);
    }
    let when = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    let secs = (when.with_timezone(&chrono::Utc) - chrono::Utc::now()).num_seconds();
    Some(secs.max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(retry_after: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(RETRY_AFTER, retry_after.parse().unwrap());
        h
    }

    #[test]
    fn retry_after_reads_seconds_and_dates() {
        assert_eq!(retry_after_secs(&headers("7")), Some(7));
        assert_eq!(retry_after_secs(&headers(" 30 ")), Some(30));
        assert_eq!(retry_after_secs(&headers("soon")), None);
        assert_eq!(retry_after_secs(&HeaderMap::new()), None);
        let later = (chrono::Utc::now() + chrono::Duration::seconds(120)).to_rfc2822();
        let secs = retry_after_secs(&headers(&later)).unwrap();
        assert!((115..=120).contains(&secs), "{secs}");
        assert_eq!(
            retry_after_secs(&headers("Wed, 21 Oct 2015 07:28:00 GMT")),
            Some(0),
            "a date in the past is now"
        );
    }

    #[test]
    fn a_429_closes_the_rung_for_its_retry_after() {
        let gate = RateGate::new("Test", Duration::from_secs(5));
        assert!(gate.admit().is_ok());
        match gate.check(StatusCode::TOO_MANY_REQUESTS, &headers("42")) {
            Err(Error::RateLimited {
                retry_after_secs, ..
            }) => assert_eq!(retry_after_secs, 42),
            other => panic!("a 429 is a rate-limit fault: {other:?}"),
        }
        match gate.admit() {
            Err(Error::RateLimited {
                retry_after_secs, ..
            }) => assert!((41..=42).contains(&retry_after_secs)),
            other => panic!("inside Retry-After the rung is skipped: {other:?}"),
        }
        let later = Instant::now() + Duration::from_secs(43);
        assert!(gate.admit_at(later).is_ok(), "after Retry-After it opens");
    }

    #[test]
    fn a_429_without_retry_after_uses_the_default_and_huge_values_are_held() {
        let gate = RateGate::new("Test", Duration::from_secs(9));
        match gate.check(StatusCode::TOO_MANY_REQUESTS, &HeaderMap::new()) {
            Err(Error::RateLimited {
                retry_after_secs, ..
            }) => assert_eq!(retry_after_secs, 9),
            other => panic!("{other:?}"),
        }
        match gate.check(StatusCode::TOO_MANY_REQUESTS, &headers("99999999")) {
            Err(Error::RateLimited {
                retry_after_secs, ..
            }) => assert_eq!(retry_after_secs, MAX_RETRY_AFTER_SECS),
            other => panic!("{other:?}"),
        }
        assert!(gate.check(StatusCode::OK, &headers("5")).is_ok());
        assert!(gate.check(StatusCode::NOT_FOUND, &HeaderMap::new()).is_ok());
    }

    /// 60 a minute: no window of 60 s lets more than 60 through.
    #[test]
    fn the_bucket_lets_at_most_its_budget_through_in_any_minute() {
        let start = Instant::now();
        let gate = RateGate::new("Test", Duration::from_secs(60)).with_per_minute(60);
        // Hammer it every 100 ms for three minutes.
        let mut admitted: Vec<Duration> = Vec::new();
        for tick in 0..1800u64 {
            let at = Duration::from_millis(tick * 100);
            if gate.admit_at(start + at).is_ok() {
                admitted.push(at);
            }
        }
        for (i, first) in admitted.iter().enumerate() {
            let in_window = admitted[i..]
                .iter()
                .take_while(|t| **t < *first + Duration::from_secs(60))
                .count();
            assert!(in_window <= 60, "{in_window} in the minute from {first:?}");
        }
        // And it is not starved: about a minute's budget per minute.
        assert!(admitted.len() >= 30 + 85, "{}", admitted.len());
    }

    #[test]
    fn an_empty_bucket_says_when_the_next_token_comes() {
        let start = Instant::now();
        let gate = RateGate::new("Test", Duration::from_secs(60)).with_per_minute(2);
        assert!(gate.admit_at(start).is_ok());
        match gate.admit_at(start) {
            Err(Error::RateLimited {
                retry_after_secs, ..
            }) => assert_eq!(retry_after_secs, 60),
            other => panic!("{other:?}"),
        }
        assert!(gate.admit_at(start + Duration::from_secs(60)).is_ok());
    }
}
