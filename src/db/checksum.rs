use bytes::{Buf, BytesMut};

use super::wal::WalHeaderMagicNumber;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Checksum(u32, u32);

impl Checksum {
    #[must_use]
    pub fn new(first: u32, second: u32) -> Self {
        Self(first, second)
    }

    #[must_use]
    pub fn compute(input: &[u8], magic_number: WalHeaderMagicNumber) -> Self {
        Self::default().compute_next(input, magic_number)
    }

    /// Computes the checksum for the next wal frame. Input is the 8 first bytes
    /// of the next wal frame header + the frame.
    #[must_use]
    pub fn compute_next(&self, input: &[u8], magic_number: WalHeaderMagicNumber) -> Self {
        let use_be = magic_number == WalHeaderMagicNumber::One;
        let mut input = BytesMut::from(input);

        let mut s0 = self.0;
        let mut s1 = self.1;
        while input.len() >= 8 {
            let (s0_add, s1_add) = if use_be {
                (input.get_u32(), input.get_u32())
            } else {
                (input.get_u32_le(), input.get_u32_le())
            };
            s0 = s0.wrapping_add(s0_add).wrapping_add(s1);
            s1 = s1.wrapping_add(s1_add).wrapping_add(s0);
        }

        Checksum(s0, s1)
    }
}
