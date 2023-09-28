use bytes::{Buf, BytesMut};
use tracing::{debug, error};

use crate::{
    error::Error,
    utils::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE},
};

use super::checksum::Checksum;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalHeaderMagicNumber {
    One = 0x377f_0683,
    Two = 0x377f_0682,
}

impl TryFrom<u32> for WalHeaderMagicNumber {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            val if val == WalHeaderMagicNumber::One as u32 => Ok(Self::One),
            val if val == WalHeaderMagicNumber::Two as u32 => Ok(Self::Two),
            val => {
                error!("Invalid magic number: {val}");
                Err(())
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct WalHeader {
    /// Magic number. 0x377f_0682 or 0x377f_0683
    magic_number: WalHeaderMagicNumber,
    /// File format version. Currently 3007000.
    file_format: u32,
    /// Database page size. Example: 1024
    page_size: u32,
    /// Checkpoint sequence number
    checkpoint_sequence_num: u32,
    /// Salt-1: random integer incremented with each checkpoint
    salt_1: u32,
    /// Salt-2: a different random number for each checkpoint
    salt_2: u32,
    /// Checksum of the first 24 bytes of header
    checksum: Checksum,
}

impl WalHeader {
    pub fn new(wal_header: &[u8]) -> Result<Self, Error> {
        if wal_header.len() < WAL_HEADER_SIZE {
            return Err(Error::WalHeaderTooShort);
        }
        let mut wal_header_bytes = BytesMut::from(wal_header);
        let magic_number = wal_header_bytes.get_u32();
        let magic_number = WalHeaderMagicNumber::try_from(magic_number)
            .map_err(|_| Error::UnexpectedMagicNumberInHeader(magic_number))?;
        let file_format = wal_header_bytes.get_u32();
        let page_size = wal_header_bytes.get_u32();
        let checkpoint_sequence_num = wal_header_bytes.get_u32();
        let salt_1 = wal_header_bytes.get_u32();
        let salt_2 = wal_header_bytes.get_u32();
        let checksum = Checksum::new(wal_header_bytes.get_u32(), wal_header_bytes.get_u32());

        // Verify checksum
        let computed_checksum = Checksum::compute(&wal_header[0..24], magic_number);
        if computed_checksum != checksum {
            return Err(Error::InvalidChecksumInWalHeader);
        }

        Ok(Self {
            magic_number,
            file_format,
            page_size,
            checkpoint_sequence_num,
            salt_1,
            salt_2,
            checksum,
        })
    }

    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.checksum
    }

    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    #[must_use]
    pub fn salt(&self) -> (u32, u32) {
        (self.salt_1, self.salt_2)
    }
}

#[derive(Debug)]
pub struct WalFrameHeader {
    /// Page number
    _page_number: u32,
    /// For commit records, the size of the database file in pages after the commit.
    /// For all other records, zero.
    _pages_after_commit: u32,
    /// Salt-1 copied from the WAL header
    _salt_1: u32,
    /// Salt-2 copied from the WAL header
    _salt_2: u32,
    /// Cumulative checksum up through and including this page
    checksum: Checksum,
}

impl WalFrameHeader {
    pub fn new(
        wal_frame_header: &[u8],
        frame: &[u8],
        wal_header: &WalHeader,
        prev_checksum: Checksum,
    ) -> Result<Self, Error> {
        if wal_frame_header.len() < WAL_FRAME_HEADER_SIZE {
            return Err(Error::WalFrameHeaderTooShort);
        }
        let mut frame_header_bytes = BytesMut::from(wal_frame_header);
        let page_number = frame_header_bytes.get_u32();
        let pages_after_commit = frame_header_bytes.get_u32();
        let salt_1 = frame_header_bytes.get_u32();
        let salt_2 = frame_header_bytes.get_u32();
        let checksum = Checksum::new(frame_header_bytes.get_u32(), frame_header_bytes.get_u32());

        // Verify salt
        if salt_1 != wal_header.salt_1 || salt_2 != wal_header.salt_2 {
            debug!("Invalid salts");
            return Err(Error::InvalidSaltsInFrameHeader);
        }

        let checksum_input = [&wal_frame_header[0..8], frame].concat();
        let computed_checksum =
            prev_checksum.compute_next(&checksum_input, wal_header.magic_number);

        // Verify checksum
        if computed_checksum != checksum {
            debug!("Invalid checksum");
            return Err(Error::InvalidSaltsInFrameHeader);
        }

        Ok(Self {
            _page_number: page_number,
            _pages_after_commit: pages_after_commit,
            _salt_1: salt_1,
            _salt_2: salt_2,
            checksum,
        })
    }

    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.checksum
    }
}

#[cfg(test)]
mod tests {
    use crate::db::wal::WalHeaderMagicNumber;

    #[test]
    fn wal_header_magic_number() {
        assert_eq!(0x377f_0683.try_into(), Ok(WalHeaderMagicNumber::One));
        assert_eq!(0x377f_0682.try_into(), Ok(WalHeaderMagicNumber::Two));
        assert_eq!(WalHeaderMagicNumber::try_from(0x377f_0681), Err(()));
    }
}
