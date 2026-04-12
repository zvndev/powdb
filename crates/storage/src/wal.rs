use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write, BufWriter};
use std::path::{Path, PathBuf};
use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    Insert   = 1,
    Update   = 2,
    Delete   = 3,
    Commit   = 4,
    Rollback = 5,
}

impl WalRecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(WalRecordType::Insert),
            2 => Some(WalRecordType::Update),
            3 => Some(WalRecordType::Delete),
            4 => Some(WalRecordType::Commit),
            5 => Some(WalRecordType::Rollback),
            _ => None,
        }
    }
}

/// WAL record header: len(4) + crc32(4) + tx_id(8) + type(1) = 17 bytes
const WAL_HEADER_SIZE: usize = 17;

/// Maximum allowed size for a single WAL record's data payload.
/// Records claiming more than 256 MB are treated as corruption and
/// stop replay — this prevents a crafted WAL from causing a
/// multi-gigabyte allocation before the CRC check can reject it.
const MAX_WAL_RECORD_SIZE: usize = 256 * 1024 * 1024;

#[derive(Debug)]
pub struct WalRecord {
    pub tx_id: u64,
    pub record_type: WalRecordType,
    pub data: Vec<u8>,
}

/// Durability mode for the WAL — analogous to SQLite's `PRAGMA synchronous`
/// combined with `journal_mode=OFF`.
///
/// * `Full` — every mutation appends a record and `flush()` calls
///   `sync_data()` so the OS guarantees the bytes hit stable storage before
///   the call returns. This is the default and the only safe choice when
///   crash recovery must be perfect.
///
/// * `Off`  — every `append()` and `flush()` is a zero-work no-op. No CRC,
///   no BufWriter, no fsync, no recovery. This matches SQLite's `:memory:`
///   semantics and is the only way to compare apples-to-apples against
///   in-memory engines in benches. Never use this in production — a crash
///   loses every mutation since the last `Catalog::checkpoint()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalSyncMode {
    #[default]
    Full,
    Off,
}

pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
    batch_size: usize,
    pending: usize,
    sync_mode: WalSyncMode,
}

impl Wal {
    pub fn create(path: &Path, batch_size: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true).write(true).read(true).truncate(true)
            .open(path)?;
        Ok(Wal {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            batch_size,
            pending: 0,
            sync_mode: WalSyncMode::default(),
        })
    }

    pub fn open(path: &Path, batch_size: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true).write(true).read(true).append(true)
            .open(path)?;
        Ok(Wal {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            batch_size,
            pending: 0,
            sync_mode: WalSyncMode::default(),
        })
    }

    /// Toggle the durability mode. See [`WalSyncMode`] for the contract.
    /// The change takes effect on the next `flush()`.
    pub fn set_sync_mode(&mut self, mode: WalSyncMode) {
        self.sync_mode = mode;
    }

    /// Returns the current sync mode (used by tests + introspection).
    pub fn sync_mode(&self) -> WalSyncMode {
        self.sync_mode
    }

    /// `true` when the WAL is in [`WalSyncMode::Off`] — i.e. every
    /// `append`/`flush` is a no-op. Catalog mutation hot paths check
    /// this BEFORE constructing WAL payloads so we don't pay
    /// `encode_row_into` + `encode_wal_payload` allocs only to throw
    /// the result away inside `append`. This is the difference between
    /// "no fsync" and "free" — the former is still 50–60% slower than
    /// the no-WAL baseline on `update_by_filter`/`delete_by_filter`,
    /// the latter matches the baseline.
    #[inline]
    pub fn is_off(&self) -> bool {
        matches!(self.sync_mode, WalSyncMode::Off)
    }

    /// Append a record to the WAL buffer. Auto-flushes when batch is full.
    ///
    /// In [`WalSyncMode::Off`] this is a zero-work no-op — see the enum's
    /// doc for the durability contract.
    pub fn append(&mut self, tx_id: u64, record_type: WalRecordType, data: &[u8]) -> io::Result<()> {
        if matches!(self.sync_mode, WalSyncMode::Off) {
            return Ok(());
        }
        let total_len = (WAL_HEADER_SIZE + data.len()) as u32;

        // Compute CRC over tx_id + type + data
        let mut crc_input = Vec::with_capacity(9 + data.len());
        crc_input.extend_from_slice(&tx_id.to_le_bytes());
        crc_input.push(record_type as u8);
        crc_input.extend_from_slice(data);
        let crc = crc32fast::hash(&crc_input);

        // Write: len + crc + tx_id + type + data
        self.writer.write_all(&total_len.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&tx_id.to_le_bytes())?;
        self.writer.write_all(&[record_type as u8])?;
        self.writer.write_all(data)?;

        self.pending += 1;
        if self.pending >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush buffered records to disk with fsync (the group commit point).
    ///
    /// No-op if nothing has been appended since the last flush. This makes
    /// it safe for the executor to unconditionally call `sync_wal` at the
    /// end of every statement — read queries pay zero fsync cost.
    pub fn flush(&mut self) -> io::Result<()> {
        let batch = self.pending;
        if batch == 0 {
            return Ok(());
        }
        self.writer.flush()?;
        // SQLite-style synchronous knob: only the explicit fsync is gated.
        // The BufWriter::flush above always runs so a process crash still
        // recovers cleanly via `read_all`.
        if matches!(self.sync_mode, WalSyncMode::Full) {
            self.writer.get_ref().sync_data()?;
        }
        self.pending = 0;
        debug!(records = batch, "wal group commit");
        Ok(())
    }

    /// Read all valid records from the WAL file.
    pub fn read_all(&self) -> io::Result<Vec<WalRecord>> {
        let mut file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        let mut pos = 0u64;
        let mut records = Vec::new();

        while pos + WAL_HEADER_SIZE as u64 <= file_len {
            file.seek(SeekFrom::Start(pos))?;

            let mut header = [0u8; WAL_HEADER_SIZE];
            if file.read_exact(&mut header).is_err() {
                break;
            }

            // These slice-to-array conversions are infallible (fixed-size
            // sub-slices of a 17-byte array) but we avoid `unwrap` to
            // satisfy the project-wide zero-panic policy.
            let total_len_bytes: [u8; 4] = match header[0..4].try_into() {
                Ok(b) => b,
                Err(_) => break,
            };
            let total_len = u32::from_le_bytes(total_len_bytes) as usize;
            let stored_crc_bytes: [u8; 4] = match header[4..8].try_into() {
                Ok(b) => b,
                Err(_) => break,
            };
            let stored_crc = u32::from_le_bytes(stored_crc_bytes);
            let tx_id_bytes: [u8; 8] = match header[8..16].try_into() {
                Ok(b) => b,
                Err(_) => break,
            };
            let tx_id = u64::from_le_bytes(tx_id_bytes);
            let record_type = match WalRecordType::from_u8(header[16]) {
                Some(rt) => rt,
                None => break,
            };

            // TASK-11: Verify the record fits within the file before
            // allocating. Catches truncated writes without any allocation.
            if pos + total_len as u64 > file_len {
                break; // Record extends beyond file — truncated write
            }

            // TASK-09: Use checked_sub to prevent integer underflow when
            // a corrupted WAL has total_len < WAL_HEADER_SIZE.
            let data_len = match total_len.checked_sub(WAL_HEADER_SIZE) {
                Some(len) => len,
                None => break, // Corrupted record — stop replay
            };

            // TASK-10: Cap allocation size before reading data. A crafted
            // WAL claiming a huge total_len would otherwise allocate
            // gigabytes before the CRC check rejects the record.
            if data_len > MAX_WAL_RECORD_SIZE {
                break; // Unreasonably large record — treat as corruption
            }

            let mut data = vec![0u8; data_len];
            if data_len > 0 {
                file.read_exact(&mut data)?;
            }

            // Verify CRC
            let mut crc_input = Vec::new();
            crc_input.extend_from_slice(&tx_id.to_le_bytes());
            crc_input.push(record_type as u8);
            crc_input.extend_from_slice(&data);
            let computed_crc = crc32fast::hash(&crc_input);

            if computed_crc != stored_crc {
                break; // Corrupted record — stop here
            }

            records.push(WalRecord { tx_id, record_type, data });
            pos += total_len as u64;
        }

        Ok(records)
    }

    /// Truncate the WAL (after checkpoint).
    pub fn truncate(&mut self) -> io::Result<()> {
        let file = OpenOptions::new()
            .write(true).read(true).truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.pending = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_wal(name: &str) -> (Wal, PathBuf) {
        let path = std::env::temp_dir().join(format!("powdb_wal_{name}_{}", std::process::id()));
        let wal = Wal::create(&path, 4).unwrap();
        (wal, path)
    }

    #[test]
    fn test_append_and_flush() {
        let (mut wal, path) = temp_wal("basic");
        wal.append(1, WalRecordType::Insert, b"row data 1").unwrap();
        wal.append(1, WalRecordType::Insert, b"row data 2").unwrap();
        wal.flush().unwrap();

        let records = wal.read_all().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].tx_id, 1);
        assert_eq!(records[0].data, b"row data 1");
        assert_eq!(records[1].data, b"row data 2");
        drop(wal);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_group_commit_auto_flush() {
        let (mut wal, path) = temp_wal("group");
        // Batch size is 4 — after 4 appends, should auto-flush
        for i in 0..4 {
            wal.append(1, WalRecordType::Insert, format!("row {i}").as_bytes()).unwrap();
        }
        // Should have flushed automatically
        let records = wal.read_all().unwrap();
        assert_eq!(records.len(), 4);
        drop(wal);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_crc_integrity() {
        let (mut wal, path) = temp_wal("crc");
        wal.append(1, WalRecordType::Insert, b"important data").unwrap();
        wal.flush().unwrap();

        let records = wal.read_all().unwrap();
        assert_eq!(records.len(), 1);
        // CRC was validated during read_all — if we get here, integrity is good
        drop(wal);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_multiple_transactions() {
        let (mut wal, path) = temp_wal("multi_tx");
        wal.append(1, WalRecordType::Insert, b"tx1 op1").unwrap();
        wal.append(2, WalRecordType::Insert, b"tx2 op1").unwrap();
        wal.append(1, WalRecordType::Commit, b"").unwrap();
        wal.append(2, WalRecordType::Commit, b"").unwrap();
        wal.flush().unwrap();

        let records = wal.read_all().unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].tx_id, 1);
        assert_eq!(records[2].tx_id, 1);
        assert_eq!(records[2].record_type, WalRecordType::Commit);
        drop(wal);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_truncate() {
        let (mut wal, path) = temp_wal("trunc");
        for i in 0..8 {
            wal.append(1, WalRecordType::Insert, format!("data {i}").as_bytes()).unwrap();
        }
        wal.flush().unwrap();
        assert_eq!(wal.read_all().unwrap().len(), 8);

        wal.truncate().unwrap();
        assert_eq!(wal.read_all().unwrap().len(), 0);
        drop(wal);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_reopen_wal() {
        let path = std::env::temp_dir().join(format!("powdb_wal_reopen_{}", std::process::id()));
        {
            let mut wal = Wal::create(&path, 128).unwrap();
            wal.append(1, WalRecordType::Insert, b"persistent").unwrap();
            wal.append(1, WalRecordType::Commit, b"").unwrap();
            wal.flush().unwrap();
        }
        {
            let wal = Wal::open(&path, 128).unwrap();
            let records = wal.read_all().unwrap();
            assert_eq!(records.len(), 2);
            assert_eq!(records[0].data, b"persistent");
            assert_eq!(records[1].record_type, WalRecordType::Commit);
        }
        std::fs::remove_file(&path).ok();
    }
}
