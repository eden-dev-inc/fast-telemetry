//! Trace and span identifiers with fast thread-local RNG.
//!
//! Uses xorshift128+ for ID generation — no external RNG dependency, no syscalls
//! per ID. Seeded from wall-clock time + thread ID for cross-thread uniqueness.

use std::cell::RefCell;
use std::fmt;

use crate::thread_id;

// ---------------------------------------------------------------------------
// TraceId
// ---------------------------------------------------------------------------

/// 128-bit trace identifier. All spans in a distributed trace share the same `TraceId`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceId(pub(crate) [u8; 16]);

impl TraceId {
    /// All-zeros sentinel indicating an invalid / absent trace.
    pub const INVALID: Self = Self([0; 16]);

    /// Generate a random trace ID using the thread-local PRNG.
    pub fn random() -> Self {
        let (a, b) = rng_next_u128();
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&a.to_le_bytes());
        bytes[8..].copy_from_slice(&b.to_le_bytes());
        Self(bytes)
    }

    /// Parse from a 32-character lowercase hex string.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 32 {
            return None;
        }
        let mut bytes = [0u8; 16];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            bytes[i] = hex_byte(chunk[0], chunk[1])?;
        }
        Some(Self(bytes))
    }

    /// Returns `true` if this is the all-zeros invalid trace ID.
    pub fn is_invalid(self) -> bool {
        self == Self::INVALID
    }

    /// Returns the raw 16-byte representation.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceId({})", self)
    }
}

// ---------------------------------------------------------------------------
// SpanId
// ---------------------------------------------------------------------------

/// 64-bit span identifier. Unique within a trace.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanId(pub(crate) [u8; 8]);

impl SpanId {
    /// All-zeros sentinel indicating an invalid / absent span (root span has no parent).
    pub const INVALID: Self = Self([0; 8]);

    /// Generate a random span ID using the thread-local PRNG.
    pub fn random() -> Self {
        Self(rng_next_u64().to_le_bytes())
    }

    /// Parse from a 16-character lowercase hex string.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 16 {
            return None;
        }
        let mut bytes = [0u8; 8];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            bytes[i] = hex_byte(chunk[0], chunk[1])?;
        }
        Some(Self(bytes))
    }

    /// Returns `true` if this is the all-zeros invalid span ID.
    pub fn is_invalid(self) -> bool {
        self == Self::INVALID
    }

    /// Returns the raw 8-byte representation.
    pub fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl fmt::Debug for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpanId({})", self)
    }
}

// ---------------------------------------------------------------------------
// Thread-local xorshift128+ PRNG
// ---------------------------------------------------------------------------

struct Xorshift128Plus {
    s0: u64,
    s1: u64,
}

impl Xorshift128Plus {
    fn next(&mut self) -> u64 {
        let mut s1 = self.s0;
        let s0 = self.s1;
        self.s0 = s0;
        s1 ^= s1 << 23;
        s1 ^= s1 >> 17;
        s1 ^= s0;
        s1 ^= s0 >> 26;
        self.s1 = s1;
        s0.wrapping_add(s1)
    }
}

thread_local! {
    static RNG: RefCell<Xorshift128Plus> = RefCell::new({
        // Seed from wall-clock time and thread ID for uniqueness across threads.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let tid = thread_id::thread_id() as u64;

        // Mix bits so nearby seeds don't produce correlated sequences.
        let s0 = nanos ^ (tid.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let s1 = nanos.wrapping_mul(0x6C62_272E_07BB_0142) ^ tid;

        // Ensure non-zero state (xorshift requires at least one non-zero).
        let s0 = if s0 == 0 { 0xDEAD_BEEF_CAFE_BABE } else { s0 };
        let s1 = if s1 == 0 { 0x0123_4567_89AB_CDEF } else { s1 };

        Xorshift128Plus { s0, s1 }
    });
}

/// Generate 128 bits of pseudo-random data as two u64s.
fn rng_next_u128() -> (u64, u64) {
    RNG.with(|rng| {
        let mut rng = rng.borrow_mut();
        let a = rng.next();
        let b = rng.next();
        (a, b)
    })
}

/// Generate 64 bits of pseudo-random data.
#[inline]
fn rng_next_u64() -> u64 {
    RNG.with(|rng| rng.borrow_mut().next())
}

// ---------------------------------------------------------------------------
// Hex helpers
// ---------------------------------------------------------------------------

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn hex_byte(hi: u8, lo: u8) -> Option<u8> {
    Some(hex_digit(hi)? << 4 | hex_digit(lo)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn trace_id_random_is_non_zero() {
        let id = TraceId::random();
        assert_ne!(id, TraceId::INVALID);
    }

    #[test]
    fn span_id_random_is_non_zero() {
        // Extremely unlikely to be all zeros, but generate several to be safe.
        for _ in 0..100 {
            let id = SpanId::random();
            assert_ne!(id, SpanId::INVALID);
        }
    }

    #[test]
    fn trace_id_uniqueness() {
        let mut set = HashSet::new();
        for _ in 0..10_000 {
            assert!(set.insert(TraceId::random()));
        }
    }

    #[test]
    fn span_id_uniqueness() {
        let mut set = HashSet::new();
        for _ in 0..10_000 {
            assert!(set.insert(SpanId::random()));
        }
    }

    #[test]
    fn trace_id_hex_roundtrip() {
        let id = TraceId::random();
        let hex = id.to_string();
        assert_eq!(hex.len(), 32);
        let parsed = TraceId::from_hex(&hex).expect("valid hex");
        assert_eq!(parsed, id);
    }

    #[test]
    fn span_id_hex_roundtrip() {
        let id = SpanId::random();
        let hex = id.to_string();
        assert_eq!(hex.len(), 16);
        let parsed = SpanId::from_hex(&hex).expect("valid hex");
        assert_eq!(parsed, id);
    }

    #[test]
    fn trace_id_from_hex_known() {
        let id = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").expect("valid");
        assert_eq!(id.to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn span_id_from_hex_known() {
        let id = SpanId::from_hex("00f067aa0ba902b7").expect("valid");
        assert_eq!(id.to_string(), "00f067aa0ba902b7");
    }

    #[test]
    fn trace_id_from_hex_rejects_bad_input() {
        assert!(TraceId::from_hex("too_short").is_none());
        assert!(TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e473x").is_none()); // bad char
        assert!(TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e47").is_none()); // 30 chars
    }

    #[test]
    fn span_id_from_hex_rejects_bad_input() {
        assert!(SpanId::from_hex("short").is_none());
        assert!(SpanId::from_hex("00f067aa0ba902bx").is_none());
    }

    #[test]
    fn invalid_sentinels() {
        assert!(TraceId::INVALID.is_invalid());
        assert!(SpanId::INVALID.is_invalid());
        assert!(!TraceId::random().is_invalid());
    }

    #[test]
    fn cross_thread_uniqueness() {
        use std::sync::Arc;
        use std::sync::Mutex;

        let ids: Arc<Mutex<Vec<TraceId>>> = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let ids = Arc::clone(&ids);
            handles.push(std::thread::spawn(move || {
                let local: Vec<TraceId> = (0..1000).map(|_| TraceId::random()).collect();
                ids.lock().expect("lock").extend(local);
            }));
        }
        for h in handles {
            h.join().expect("thread join");
        }

        let all = ids.lock().expect("lock");
        let set: HashSet<_> = all.iter().collect();
        assert_eq!(set.len(), all.len(), "duplicate trace IDs across threads");
    }
}
