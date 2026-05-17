use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Instant;

use crate::middleware::{Middleware, ConnectionContext};

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

pub struct RateLimiter {
    max_requests: f64,
    refill_rate: f64,
    buckets: Mutex<HashMap<IpAddr, TokenBucket>>,
    last_cleanup: Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(max_requests: f64, refill_rate: f64) -> Self {
        Self {
            max_requests,
            refill_rate,
            buckets: Mutex::new(HashMap::new()),
            last_cleanup: Mutex::new(Instant::now()),
        }
    }

    fn filter(&self, client_ip: IpAddr) -> bool {
        let mut guard = self.buckets.lock().unwrap();
        let now = Instant::now();

        // Garbage collection for old IP addresses (every 60 seconds)
        let mut cleanup_guard = self.last_cleanup.lock().unwrap();
        if now.duration_since(*cleanup_guard).as_secs() > 60 {
            guard.retain(|_, b| {
                let elapsed = now.duration_since(b.last_refill).as_secs_f64();
                let current_tokens = (b.tokens + elapsed * self.refill_rate).min(self.max_requests);
                // Retain only if the bucket is not full
                current_tokens < self.max_requests
            });
            *cleanup_guard = now;
        }
        drop(cleanup_guard);

        let bucket = guard.entry(client_ip).or_insert_with(|| TokenBucket {
            tokens: self.max_requests,
            last_refill: now,
        });

        // Refill tokens based on time elapsed since last check
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_rate).min(self.max_requests);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true // Allow request
        } else {
            false // Exceeded limit, block connection
        }
    }
}

impl Middleware for RateLimiter {
    fn name(&self) -> &'static str {
        "RateLimiter"
    }

    fn handle(&self, ctx: &mut ConnectionContext) -> bool {
        self.filter(ctx.client_ip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use std::time::Duration;

    #[test]
    fn test_rate_limiter_basics() {
        let limiter = RateLimiter::new(2.0, 1.0); // max 2 tokens, refills 1 token/sec
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        // 1st request should be allowed (2.0 -> 1.0 tokens)
        assert!(limiter.filter(ip));

        // 2nd request should be allowed (1.0 -> 0.0 tokens)
        assert!(limiter.filter(ip));

        // 3rd request should be blocked (0.0 tokens left)
        assert!(!limiter.filter(ip));
    }

    #[test]
    fn test_rate_limiter_independent_ips() {
        let limiter = RateLimiter::new(1.0, 1.0);
        let ip1: IpAddr = "127.0.0.1".parse().unwrap();
        let ip2: IpAddr = "192.168.1.1".parse().unwrap();

        // IP1 consumes its token
        assert!(limiter.filter(ip1));
        assert!(!limiter.filter(ip1));

        // IP2 should still have its token (independent buckets)
        assert!(limiter.filter(ip2));
        assert!(!limiter.filter(ip2));
    }

    #[test]
    fn test_rate_limiter_refill() {
        let limiter = RateLimiter::new(1.0, 100.0); // refills 100 tokens per second!
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        assert!(limiter.filter(ip));
        assert!(!limiter.filter(ip)); // Empty bucket!

        // Sleep briefly to let the high refill rate add back a token
        std::thread::sleep(Duration::from_millis(15));

        // Should refill and allow request again!
        assert!(limiter.filter(ip));
    }

    #[test]
    fn test_middleware_integration() {
        let limiter = RateLimiter::new(1.0, 1.0);
        let mut ctx = ConnectionContext::new("127.0.0.1".parse().unwrap());

        // First handle passes
        assert!(limiter.handle(&mut ctx));

        // Second handle gets rate limited
        assert!(!limiter.handle(&mut ctx));
    }
}
