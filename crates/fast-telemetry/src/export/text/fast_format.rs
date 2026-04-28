//! Fast numeric-to-string formatting for the text exporters.
//!
//! `core::fmt::Display` goes through the `Formatter` machinery (padding, alignment,
//! locale checks, dynamic dispatch). For metric exports that emit thousands of
//! integers and floats per scrape, `itoa` and `ryu` are 3-8x faster because they
//! write directly to a stack buffer. This trait centralizes that dispatch.
//!
//! `FastFormat` is impl'd for the primitive numeric types this crate emits.
//! Floats use `ryu`'s shortest-roundtrip form, which is canonical for both
//! Prometheus exposition and DogStatsD.

/// Internal trait for fast numeric serialization. Exposed only because the
/// `__write_dogstatsd*` macro-support functions reference it in their public
/// signatures; not part of the stable API.
#[doc(hidden)]
pub trait FastFormat {
    fn fast_push(self, output: &mut String);
}

macro_rules! impl_int {
    ($($t:ty),*) => {
        $(
            impl FastFormat for $t {
                #[inline]
                fn fast_push(self, output: &mut String) {
                    let mut buf = itoa::Buffer::new();
                    output.push_str(buf.format(self));
                }
            }
        )*
    };
}

impl_int!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

impl FastFormat for f32 {
    #[inline]
    fn fast_push(self, output: &mut String) {
        let mut buf = ryu::Buffer::new();
        output.push_str(buf.format(self));
    }
}

impl FastFormat for f64 {
    #[inline]
    fn fast_push(self, output: &mut String) {
        let mut buf = ryu::Buffer::new();
        output.push_str(buf.format(self));
    }
}

/// DogStatsD's compact form: whole numbers as integers ("1" not "1.0"),
/// fractions as canonical f64.
#[inline]
pub(crate) fn push_f64_compact(output: &mut String, value: f64) {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        (value as i64).fast_push(output);
    } else {
        value.fast_push(output);
    }
}
