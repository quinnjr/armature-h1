//! Per-connection deadlines.
//!
//! One [`tokio::time::Sleep`] lives per connection and is reused across every
//! phase — header read, body read, write, idle — via `Sleep::reset`. That is
//! what removes the per-request timer allocation a `tokio::time::timeout` call
//! around each read would incur.
//!
//! A bespoke timing wheel is deliberately *not* built here: tokio's timer driver
//! already is a hierarchical wheel, so reimplementing one would add code without
//! adding capability. Coarsening each deadline up to a tick boundary is what
//! keeps that wheel's slot churn low.
//!
//! Arming is lazy. A keep-alive request moves through four phases, and
//! resetting the `Sleep` on each one re-registered it with the timer driver
//! four times per request — the largest single cost on the keep-alive path in a
//! profile, ahead of parsing. So [`ConnDeadline::arm`] only records the logical
//! deadline, and touches the timer only when that deadline is *earlier* than
//! the one the timer already holds. A timer that fires before the logical
//! deadline is re-pointed at it and keeps waiting. The deadline still never
//! fires early, and never later than it would have; in steady state the timer
//! is re-registered once per expiry rather than once per phase.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Sleep};

/// How far into the future a disarmed deadline is pushed.
///
/// Long enough never to fire in practice, short enough to stay well inside
/// tokio's representable timer range.
const FAR_FUTURE: Duration = Duration::from_secs(86_400 * 365);

/// A reusable, coarsened deadline for one connection.
pub struct ConnDeadline {
    /// Always at or before `target` while armed, so it can only wake early.
    sleep: Pin<Box<Sleep>>,
    tick: Duration,
    /// The logical deadline; `None` while disarmed.
    target: Option<Instant>,
}

impl ConnDeadline {
    /// Create a disarmed deadline coarsened to `tick`.
    pub fn new(tick: Duration) -> Self {
        let tick = if tick.is_zero() {
            Duration::from_millis(1)
        } else {
            tick
        };
        Self {
            sleep: Box::pin(tokio::time::sleep(FAR_FUTURE)),
            tick,
            target: None,
        }
    }

    /// Arm the deadline for `after` from now, rounded up to the next tick.
    ///
    /// Rounding up rather than down matters: a deadline must never fire early,
    /// or a slow-but-legitimate client gets a 408 it did not earn.
    pub fn arm(&mut self, after: Duration) {
        let target = Instant::now() + round_up(after, self.tick);
        self.target = Some(target);
        // A later deadline leaves the timer where it is: it wakes early and
        // `poll_expired` re-points it. Only an earlier one has to move it now.
        if self.sleep.deadline() > target {
            self.sleep.as_mut().reset(target);
        }
    }

    /// Stop the deadline from firing until it is armed again.
    ///
    /// The timer itself is left alone; if it wakes meanwhile, `poll_expired`
    /// sees nothing armed and reports nothing.
    pub fn disarm(&mut self) {
        self.target = None;
    }

    /// Poll the deadline.
    ///
    /// A disarmed deadline is always `Pending`, without registering a waker:
    /// nothing can make it fire until the owner arms it and polls again.
    #[inline]
    pub fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(target) = self.target else {
            return Poll::Pending;
        };
        loop {
            if self.sleep.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            if self.sleep.deadline() >= target {
                return Poll::Ready(());
            }
            // Woke for an earlier deadline that was since replaced by a later
            // one: chase the real one. Polling again registers the waker.
            self.sleep.as_mut().reset(target);
        }
    }

    /// Wait for the armed deadline to pass.
    ///
    /// Borrows mutably rather than consuming, so the same timer serves the next
    /// phase.
    pub async fn expired(&mut self) {
        Expired { d: self }.await
    }
}

impl std::fmt::Debug for ConnDeadline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnDeadline")
            .field("tick", &self.tick)
            .field("deadline", &self.target)
            .finish()
    }
}

/// Future returned by [`ConnDeadline::expired`].
struct Expired<'a> {
    d: &'a mut ConnDeadline,
}

impl Future for Expired<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.d.poll_expired(cx)
    }
}

/// Round `d` up to the next multiple of `tick`.
///
/// In `u64` nanoseconds when both fit (about 584 years), because this runs on
/// every arm and a `u128` division is a library call.
fn round_up(d: Duration, tick: Duration) -> Duration {
    let (Ok(target), Ok(n)) = (u64::try_from(d.as_nanos()), u64::try_from(tick.as_nanos())) else {
        // Beyond `u64` nanoseconds either way: saturate, as the `u128`
        // arithmetic this replaced did.
        return if d.is_zero() {
            d
        } else {
            Duration::from_nanos(u64::MAX)
        };
    };
    if n == 0 {
        return d;
    }
    Duration::from_nanos(target.div_ceil(n).saturating_mul(n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;

    const TICK: Duration = Duration::from_millis(100);

    /// Whether the deadline has already fired, without waiting for it.
    async fn is_expired(d: &mut ConnDeadline) -> bool {
        poll_fn(|cx| Poll::Ready(d.poll_expired(cx).is_ready())).await
    }

    #[tokio::test(start_paused = true)]
    async fn expires_after_the_armed_duration() {
        let mut d = ConnDeadline::new(TICK);
        d.arm(Duration::from_secs(1));
        tokio::time::advance(Duration::from_millis(1001)).await;
        assert!(is_expired(&mut d).await);
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_expire_early() {
        let mut d = ConnDeadline::new(TICK);
        d.arm(Duration::from_secs(1));
        tokio::time::advance(Duration::from_millis(900)).await;
        assert!(!is_expired(&mut d).await);
    }

    #[tokio::test(start_paused = true)]
    async fn rearming_extends_the_deadline() {
        let mut d = ConnDeadline::new(TICK);
        d.arm(Duration::from_secs(1));
        tokio::time::advance(Duration::from_millis(500)).await;
        d.arm(Duration::from_secs(1));
        // The original deadline moment passes without expiry.
        tokio::time::advance(Duration::from_millis(600)).await;
        assert!(!is_expired(&mut d).await);
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(is_expired(&mut d).await);
    }

    #[tokio::test(start_paused = true)]
    async fn disarm_prevents_expiry() {
        let mut d = ConnDeadline::new(TICK);
        d.arm(Duration::from_millis(100));
        d.disarm();
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert!(!is_expired(&mut d).await);
    }

    /// Coarsening rounds up, never down: a deadline that fires early would give
    /// a slow-but-legitimate client a 408 it did not earn.
    #[tokio::test(start_paused = true)]
    async fn coarsens_up_to_the_tick_boundary() {
        let mut d = ConnDeadline::new(TICK);
        d.arm(Duration::from_millis(10));
        tokio::time::advance(Duration::from_millis(11)).await;
        assert!(!is_expired(&mut d).await, "must not fire at 10ms");
        tokio::time::advance(Duration::from_millis(95)).await;
        assert!(is_expired(&mut d).await, "must fire by the 100ms boundary");
    }

    #[tokio::test(start_paused = true)]
    async fn reuses_one_timer_across_many_arms() {
        let mut d = ConnDeadline::new(TICK);
        for _ in 0..1000 {
            d.arm(Duration::from_millis(100));
            d.expired().await;
        }
        assert!(is_expired(&mut d).await);
    }

    /// The timer still holds the earlier deadline after a later arm; waking
    /// for it must not count as expiry.
    #[tokio::test(start_paused = true)]
    async fn a_later_rearm_is_not_cut_short_by_the_earlier_timer() {
        let mut d = ConnDeadline::new(TICK);
        d.arm(Duration::from_secs(1));
        assert!(!is_expired(&mut d).await);
        d.arm(Duration::from_secs(5));
        tokio::time::advance(Duration::from_millis(1500)).await;
        assert!(
            !is_expired(&mut d).await,
            "fired at the superseded deadline"
        );
        tokio::time::advance(Duration::from_millis(3400)).await;
        assert!(!is_expired(&mut d).await, "fired early");
        tokio::time::advance(Duration::from_millis(101)).await;
        assert!(is_expired(&mut d).await);
    }

    #[tokio::test(start_paused = true)]
    async fn an_earlier_rearm_moves_the_timer() {
        let mut d = ConnDeadline::new(TICK);
        d.arm(Duration::from_secs(5));
        assert!(!is_expired(&mut d).await);
        d.arm(Duration::from_secs(1));
        tokio::time::advance(Duration::from_millis(1001)).await;
        assert!(is_expired(&mut d).await);
    }

    /// A timer that woke while disarmed is in the past when the next arm
    /// lands; it must be re-pointed rather than reported as expired.
    #[tokio::test(start_paused = true)]
    async fn a_stale_wake_while_disarmed_does_not_expire_the_next_arm() {
        let mut d = ConnDeadline::new(TICK);
        d.arm(Duration::from_millis(100));
        assert!(!is_expired(&mut d).await);
        d.disarm();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!is_expired(&mut d).await);
        d.arm(Duration::from_secs(1));
        assert!(!is_expired(&mut d).await, "stale wake reported as expiry");
        tokio::time::advance(Duration::from_millis(999)).await;
        assert!(!is_expired(&mut d).await);
        tokio::time::advance(Duration::from_millis(2)).await;
        assert!(is_expired(&mut d).await);
    }

    #[test]
    fn round_up_saturates_beyond_u64_nanoseconds() {
        assert_eq!(
            round_up(Duration::MAX, TICK),
            Duration::from_nanos(u64::MAX)
        );
        assert_eq!(round_up(Duration::ZERO, Duration::MAX), Duration::ZERO);
    }

    #[test]
    fn round_up_is_exact_on_boundaries() {
        assert_eq!(round_up(Duration::from_millis(100), TICK), TICK);
        assert_eq!(
            round_up(Duration::from_millis(101), TICK),
            Duration::from_millis(200)
        );
        assert_eq!(round_up(Duration::ZERO, TICK), Duration::ZERO);
        assert_eq!(
            round_up(Duration::from_millis(1), TICK),
            Duration::from_millis(100)
        );
    }
}
