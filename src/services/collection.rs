//! Service collection with failover support.
//!
//! Provides the `ServiceCollection` type that maintains an ordered list of
//! service providers and handles failover between them.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Maximum number of call history entries to keep per provider.
const MAX_CALL_HISTORY: usize = 32;

/// Maximum number of reset intervals to keep.
const MAX_RESET_COUNTS: usize = 32;

/// Configuration for adaptive timeout behavior.
#[derive(Debug, Clone)]
pub struct AdaptiveTimeoutConfig {
    /// Minimum timeout in milliseconds.
    pub min_timeout_ms: u64,
    /// Maximum timeout in milliseconds.
    pub max_timeout_ms: u64,
    /// Multiplier applied to average response time.
    pub multiplier: f64,
    /// Initial timeout in milliseconds.
    pub initial_timeout_ms: u64,
}

impl Default for AdaptiveTimeoutConfig {
    fn default() -> Self {
        Self {
            min_timeout_ms: 5_000,
            max_timeout_ms: 60_000,
            multiplier: 2.0,
            initial_timeout_ms: 30_000,
        }
    }
}

/// A collection of service providers with failover support.
///
/// Maintains an ordered list of providers and tracks call statistics.
/// When a call fails, it automatically tries the next provider.
pub struct ServiceCollection<S> {
    /// Name of this service collection (e.g., "getMerklePath").
    pub service_name: String,

    /// Ordered list of service providers.
    services: Vec<NamedService<S>>,

    /// Current index in the service list.
    index: usize,

    /// Start time of current statistics interval.
    #[allow(dead_code)]
    since: DateTime<Utc>,

    /// History of calls by provider name.
    history_by_provider: HashMap<String, ProviderCallHistoryInternal>,

    /// Adaptive timeout configuration.
    timeout_config: AdaptiveTimeoutConfig,

    /// EMA of response times (f64 bits in AtomicU64).
    avg_response_ms: AtomicU64,
}

/// A named service provider.
pub struct NamedService<S> {
    /// Provider name (e.g., "WhatsOnChain", "ARC").
    pub name: String,

    /// The service instance.
    pub service: S,
}

/// Information about a service call being made.
pub struct ServiceToCall<'a, S> {
    /// Name of the service collection.
    pub service_name: &'a str,

    /// Name of the provider.
    pub provider_name: &'a str,

    /// Reference to the service.
    pub service: &'a S,

    /// Call metadata for tracking.
    pub call: ServiceCall,
}

/// Metadata for a single service call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceCall {
    /// When the call was initiated.
    pub when: DateTime<Utc>,

    /// Duration in milliseconds.
    pub msecs: u64,

    /// Whether the call succeeded.
    pub success: bool,

    /// Brief result description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,

    /// Error information if failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ServiceCallError>,
}

/// Error information from a service call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceCallError {
    /// Error message.
    pub message: String,

    /// Error code.
    pub code: String,
}

impl ServiceCall {
    /// Create a new call record starting now.
    pub fn new() -> Self {
        Self {
            when: Utc::now(),
            msecs: 0,
            success: false,
            result: None,
            error: None,
        }
    }

    /// Mark the call as complete and calculate duration.
    pub fn complete(&mut self) {
        let duration = Utc::now() - self.when;
        self.msecs = duration.num_milliseconds().max(0) as u64;
    }

    /// Mark as successful with optional result.
    pub fn mark_success(&mut self, result: Option<String>) {
        self.complete();
        self.success = true;
        self.result = result;
        self.error = None;
    }

    /// Mark as failed with optional result.
    pub fn mark_failure(&mut self, result: Option<String>) {
        self.complete();
        self.success = false;
        self.result = result;
        self.error = None;
    }

    /// Mark as failed with error.
    pub fn mark_error(&mut self, message: &str, code: &str) {
        self.complete();
        self.success = false;
        self.result = None;
        self.error = Some(ServiceCallError {
            message: message.to_string(),
            code: code.to_string(),
        });
    }
}

impl Default for ServiceCall {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal call history tracking for a provider.
struct ProviderCallHistoryInternal {
    service_name: String,
    provider_name: String,
    calls: Vec<ServiceCall>,
    total_counts: CallCounts,
    reset_counts: Vec<CallCounts>,
}

/// Call counts for statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallCounts {
    /// Number of successful calls.
    pub success: u64,

    /// Number of failed calls (including errors).
    pub failure: u64,

    /// Number of calls that threw errors.
    pub error: u64,

    /// Start of this counting interval.
    pub since: DateTime<Utc>,

    /// End of this counting interval.
    pub until: DateTime<Utc>,
}

impl CallCounts {
    fn new(since: DateTime<Utc>) -> Self {
        Self {
            success: 0,
            failure: 0,
            error: 0,
            since,
            until: since,
        }
    }
}

/// Call history for a specific provider (serializable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCallHistory {
    /// Service collection name.
    pub service_name: String,

    /// Provider name.
    pub provider_name: String,

    /// Recent calls.
    pub calls: Vec<ServiceCall>,

    /// Total statistics since creation.
    pub total_counts: CallCounts,

    /// Statistics by reset interval.
    pub reset_counts: Vec<CallCounts>,
}

/// Complete call history for a service collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceCallHistory {
    /// Service collection name.
    pub service_name: String,

    /// History by provider name.
    pub history_by_provider: HashMap<String, ProviderCallHistory>,
}

impl<S> ServiceCollection<S> {
    /// Create a new service collection.
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            services: Vec::new(),
            index: 0,
            since: Utc::now(),
            history_by_provider: HashMap::new(),
            timeout_config: AdaptiveTimeoutConfig::default(),
            avg_response_ms: AtomicU64::new(0),
        }
    }

    /// Create with custom timeout config.
    pub fn with_timeout_config(
        service_name: impl Into<String>,
        timeout_config: AdaptiveTimeoutConfig,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            services: Vec::new(),
            index: 0,
            since: Utc::now(),
            history_by_provider: HashMap::new(),
            timeout_config,
            avg_response_ms: AtomicU64::new(0),
        }
    }

    /// Add a service provider to the collection.
    pub fn add(&mut self, name: impl Into<String>, service: S) -> &mut Self {
        self.services.push(NamedService {
            name: name.into(),
            service,
        });
        self
    }

    /// Builder pattern: add and return self.
    pub fn with(mut self, name: impl Into<String>, service: S) -> Self {
        self.add(name, service);
        self
    }

    /// Remove a provider by name.
    pub fn remove(&mut self, name: &str) {
        self.services.retain(|s| s.name != name);
        if self.index >= self.services.len() && !self.services.is_empty() {
            self.index = 0;
        }
    }

    /// Get the number of providers.
    pub fn count(&self) -> usize {
        self.services.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    /// Get current provider name.
    pub fn current_name(&self) -> Option<&str> {
        self.services.get(self.index).map(|s| s.name.as_str())
    }

    /// Get current provider service.
    pub fn current_service(&self) -> Option<&S> {
        self.services.get(self.index).map(|s| &s.service)
    }

    /// Get service to call at current index.
    pub fn service_to_call(&self) -> Option<ServiceToCall<'_, S>> {
        self.get_service_to_call(self.index)
    }

    /// Get service to call at specific index.
    pub fn get_service_to_call(&self, index: usize) -> Option<ServiceToCall<'_, S>> {
        self.services.get(index).map(|s| ServiceToCall {
            service_name: &self.service_name,
            provider_name: &s.name,
            service: &s.service,
            call: ServiceCall::new(),
        })
    }

    /// Get all services to call.
    pub fn all_services_to_call(&self) -> Vec<ServiceToCall<'_, S>> {
        self.services
            .iter()
            .map(|s| ServiceToCall {
                service_name: &self.service_name,
                provider_name: &s.name,
                service: &s.service,
                call: ServiceCall::new(),
            })
            .collect()
    }

    /// Get all services as owned copies (service name, provider name, cloned service).
    /// This is useful when you need to iterate without holding a lock.
    pub fn all_services_owned(&self) -> Vec<(String, String, S)>
    where
        S: Clone,
    {
        self.services
            .iter()
            .map(|s| (self.service_name.clone(), s.name.clone(), s.service.clone()))
            .collect()
    }

    /// Get all services starting from the current index in round-robin order.
    /// This respects the current position and returns services rotated accordingly.
    pub fn all_services_from_current(&self) -> Vec<(String, String, S)>
    where
        S: Clone,
    {
        if self.services.is_empty() {
            return Vec::new();
        }

        let len = self.services.len();
        let mut result = Vec::with_capacity(len);

        for i in 0..len {
            let idx = (self.index + i) % len;
            let s = &self.services[idx];
            result.push((self.service_name.clone(), s.name.clone(), s.service.clone()));
        }

        result
    }

    /// Move to the next provider, wrapping around.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> usize {
        if !self.services.is_empty() {
            self.index = (self.index + 1) % self.services.len();
        }
        self.index
    }

    /// Reset to the first provider.
    pub fn reset(&mut self) {
        self.index = 0;
    }

    /// Move a provider to the end of the list (de-prioritize).
    pub fn move_to_last(&mut self, name: &str) {
        if let Some(pos) = self.services.iter().position(|s| s.name == name) {
            let service = self.services.remove(pos);
            self.services.push(service);

            // Adjust index if needed
            if self.index > pos && self.index > 0 {
                self.index -= 1;
            } else if self.index == pos {
                self.index = 0;
            }
        }
    }

    /// Record a successful call.
    pub fn add_call_success(&mut self, provider_name: &str, call: ServiceCall) {
        let h = self.get_or_create_history(provider_name);
        h.calls.insert(0, call);
        h.calls.truncate(MAX_CALL_HISTORY);
        h.total_counts.success += 1;
        h.total_counts.until = Utc::now();
        if let Some(rc) = h.reset_counts.first_mut() {
            rc.success += 1;
            rc.until = Utc::now();
        }
    }

    /// Record a failed call (no error thrown).
    pub fn add_call_failure(&mut self, provider_name: &str, call: ServiceCall) {
        let h = self.get_or_create_history(provider_name);
        h.calls.insert(0, call);
        h.calls.truncate(MAX_CALL_HISTORY);
        h.total_counts.failure += 1;
        h.total_counts.until = Utc::now();
        if let Some(rc) = h.reset_counts.first_mut() {
            rc.failure += 1;
            rc.until = Utc::now();
        }
    }

    /// Record a call that threw an error.
    pub fn add_call_error(&mut self, provider_name: &str, call: ServiceCall) {
        let h = self.get_or_create_history(provider_name);
        h.calls.insert(0, call);
        h.calls.truncate(MAX_CALL_HISTORY);
        h.total_counts.failure += 1;
        h.total_counts.error += 1;
        h.total_counts.until = Utc::now();
        if let Some(rc) = h.reset_counts.first_mut() {
            rc.failure += 1;
            rc.error += 1;
            rc.until = Utc::now();
        }
    }

    /// Get adaptive timeout config.
    pub fn timeout_config(&self) -> &AdaptiveTimeoutConfig {
        &self.timeout_config
    }

    /// Set adaptive timeout config.
    pub fn set_timeout_config(&mut self, config: AdaptiveTimeoutConfig) {
        self.timeout_config = config;
    }

    /// Get current adaptive timeout.
    pub fn get_current_timeout(&self) -> Duration {
        let avg_bits = self.avg_response_ms.load(Ordering::Relaxed);
        let avg = f64::from_bits(avg_bits);
        if avg <= 0.0 {
            return Duration::from_millis(self.timeout_config.initial_timeout_ms);
        }
        let computed = avg * self.timeout_config.multiplier;
        let clamped = computed
            .max(self.timeout_config.min_timeout_ms as f64)
            .min(self.timeout_config.max_timeout_ms as f64);
        Duration::from_millis(clamped as u64)
    }

    /// Record response time, update EMA (0.7/0.3).
    pub fn record_response_time(&self, elapsed_ms: u64) {
        let sample = elapsed_ms as f64;
        loop {
            let old_bits = self.avg_response_ms.load(Ordering::Relaxed);
            let old_avg = f64::from_bits(old_bits);
            let new_avg = if old_avg <= 0.0 {
                sample
            } else {
                old_avg * 0.7 + sample * 0.3
            };
            let new_bits = new_avg.to_bits();
            match self.avg_response_ms.compare_exchange_weak(
                old_bits,
                new_bits,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
    }

    /// Get current avg response time (None if no data).
    pub fn avg_response_ms(&self) -> Option<f64> {
        let bits = self.avg_response_ms.load(Ordering::Relaxed);
        let avg = f64::from_bits(bits);
        if avg <= 0.0 {
            None
        } else {
            Some(avg)
        }
    }

    /// Get call history, optionally resetting counters.
    pub fn get_call_history(&mut self, reset: bool) -> ServiceCallHistory {
        let now = Utc::now();

        let history_by_provider = self
            .history_by_provider
            .iter_mut()
            .map(|(name, h)| {
                let history = ProviderCallHistory {
                    service_name: h.service_name.clone(),
                    provider_name: h.provider_name.clone(),
                    calls: h.calls.clone(),
                    total_counts: h.total_counts.clone(),
                    reset_counts: h.reset_counts.clone(),
                };

                if reset {
                    // Complete current interval
                    if let Some(rc) = h.reset_counts.first_mut() {
                        rc.until = now;
                    }

                    // Start new interval
                    h.reset_counts.insert(0, CallCounts::new(now));
                    h.reset_counts.truncate(MAX_RESET_COUNTS);
                }

                (name.clone(), history)
            })
            .collect();

        ServiceCallHistory {
            service_name: self.service_name.clone(),
            history_by_provider,
        }
    }

    fn get_or_create_history(&mut self, provider_name: &str) -> &mut ProviderCallHistoryInternal {
        let now = Utc::now();
        let service_name = self.service_name.clone();

        self.history_by_provider
            .entry(provider_name.to_string())
            .or_insert_with(|| ProviderCallHistoryInternal {
                service_name,
                provider_name: provider_name.to_string(),
                calls: Vec::new(),
                total_counts: CallCounts::new(now),
                reset_counts: vec![CallCounts::new(now)],
            })
    }
}

impl<S: Clone> ServiceCollection<S> {
    /// Clone this collection (useful for isolated operations).
    pub fn clone_collection(&self) -> Self {
        Self {
            service_name: self.service_name.clone(),
            services: self
                .services
                .iter()
                .map(|s| NamedService {
                    name: s.name.clone(),
                    service: s.service.clone(),
                })
                .collect(),
            index: 0, // Always start at beginning for clones
            since: Utc::now(),
            history_by_provider: HashMap::new(),
            timeout_config: self.timeout_config.clone(),
            avg_response_ms: AtomicU64::new(self.avg_response_ms.load(Ordering::Relaxed)),
        }
    }
}

/// Thread-safe wrapper for ServiceCollection.
pub struct SharedServiceCollection<S>(pub Arc<RwLock<ServiceCollection<S>>>);

impl<S> SharedServiceCollection<S> {
    pub fn new(collection: ServiceCollection<S>) -> Self {
        Self(Arc::new(RwLock::new(collection)))
    }

    pub fn read(&self) -> crate::Result<std::sync::RwLockReadGuard<'_, ServiceCollection<S>>> {
        crate::lock_utils::lock_read(&self.0)
    }

    pub fn write(&self) -> crate::Result<std::sync::RwLockWriteGuard<'_, ServiceCollection<S>>> {
        crate::lock_utils::lock_write(&self.0)
    }
}

impl<S> Clone for SharedServiceCollection<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_collection_basic() {
        let mut collection = ServiceCollection::<String>::new("test")
            .with("provider1", "service1".to_string())
            .with("provider2", "service2".to_string());

        assert_eq!(collection.count(), 2);
        assert_eq!(collection.current_name(), Some("provider1"));

        collection.next();
        assert_eq!(collection.current_name(), Some("provider2"));

        collection.next();
        assert_eq!(collection.current_name(), Some("provider1")); // Wrapped

        collection.reset();
        assert_eq!(collection.current_name(), Some("provider1"));
    }

    #[test]
    fn test_service_collection_remove() {
        let mut collection = ServiceCollection::<String>::new("test")
            .with("provider1", "service1".to_string())
            .with("provider2", "service2".to_string())
            .with("provider3", "service3".to_string());

        collection.next(); // Move to provider2 (index 1)
        collection.remove("provider2"); // Removes item at index 1, list becomes [p1, p3]
                                        // Index 1 now points to provider3 (the item that was at index 2)

        assert_eq!(collection.count(), 2);
        assert_eq!(collection.current_name(), Some("provider3"));
    }

    #[test]
    fn test_service_collection_move_to_last() {
        let mut collection = ServiceCollection::<String>::new("test")
            .with("provider1", "service1".to_string())
            .with("provider2", "service2".to_string())
            .with("provider3", "service3".to_string());

        collection.move_to_last("provider1");

        let names: Vec<_> = collection
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(names, vec!["provider2", "provider3", "provider1"]);
    }

    #[test]
    fn test_call_history_tracking() {
        let mut collection =
            ServiceCollection::<String>::new("test").with("provider1", "service1".to_string());

        let mut call = ServiceCall::new();
        call.mark_success(Some("ok".to_string()));
        collection.add_call_success("provider1", call);

        let mut call = ServiceCall::new();
        call.mark_failure(Some("not found".to_string()));
        collection.add_call_failure("provider1", call);

        let mut call = ServiceCall::new();
        call.mark_error("Connection failed", "ECONNRESET");
        collection.add_call_error("provider1", call);

        let history = collection.get_call_history(false);
        let provider_history = history.history_by_provider.get("provider1").unwrap();

        assert_eq!(provider_history.total_counts.success, 1);
        assert_eq!(provider_history.total_counts.failure, 2);
        assert_eq!(provider_history.total_counts.error, 1);
        assert_eq!(provider_history.calls.len(), 3);
    }

    #[test]
    fn test_adaptive_timeout_defaults() {
        let config = AdaptiveTimeoutConfig::default();
        assert_eq!(config.min_timeout_ms, 5_000);
        assert_eq!(config.max_timeout_ms, 60_000);
        assert!((config.multiplier - 2.0).abs() < f64::EPSILON);
        assert_eq!(config.initial_timeout_ms, 30_000);
    }

    #[test]
    fn test_adaptive_timeout_initial() {
        let collection = ServiceCollection::<String>::new("test");
        let timeout = collection.get_current_timeout();
        assert_eq!(timeout, Duration::from_millis(30_000));
    }

    #[test]
    fn test_adaptive_timeout_after_recording() {
        let collection = ServiceCollection::<String>::new("test");
        collection.record_response_time(100);
        let timeout = collection.get_current_timeout();
        assert_eq!(timeout, Duration::from_millis(5_000));
    }

    #[test]
    fn test_adaptive_timeout_within_bounds() {
        let config = AdaptiveTimeoutConfig {
            min_timeout_ms: 1_000,
            max_timeout_ms: 10_000,
            multiplier: 2.0,
            initial_timeout_ms: 5_000,
        };
        let collection = ServiceCollection::<String>::with_timeout_config("test", config);
        collection.record_response_time(3000);
        assert_eq!(
            collection.get_current_timeout(),
            Duration::from_millis(6_000)
        );
    }

    #[test]
    fn test_adaptive_timeout_max_clamp() {
        let config = AdaptiveTimeoutConfig {
            min_timeout_ms: 1_000,
            max_timeout_ms: 10_000,
            multiplier: 2.0,
            initial_timeout_ms: 5_000,
        };
        let collection = ServiceCollection::<String>::with_timeout_config("test", config);
        collection.record_response_time(50_000);
        assert_eq!(
            collection.get_current_timeout(),
            Duration::from_millis(10_000)
        );
    }

    #[test]
    fn test_ema_convergence() {
        let collection = ServiceCollection::<String>::new("test");
        collection.record_response_time(1000);
        assert!((collection.avg_response_ms().unwrap() - 1000.0).abs() < 0.01);
        collection.record_response_time(1000);
        assert!((collection.avg_response_ms().unwrap() - 1000.0).abs() < 0.01);
        collection.record_response_time(2000);
        assert!((collection.avg_response_ms().unwrap() - 1300.0).abs() < 1.0);
    }

    #[test]
    fn test_avg_response_none_initially() {
        let collection = ServiceCollection::<String>::new("test");
        assert!(collection.avg_response_ms().is_none());
    }
}
