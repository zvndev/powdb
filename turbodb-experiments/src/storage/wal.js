/**
 * Write-Ahead Log (WAL)
 * 
 * Every durable database must have some form of WAL. The question is:
 * how much does durability cost?
 * 
 * This implements three modes:
 * 1. no_wal     — writes go directly to data, no crash safety
 * 2. wal_fsync  — every write goes to WAL + fsync (safest, slowest)
 * 3. wal_batch  — writes accumulate, fsync once per batch (group commit)
 * 
 * PostgreSQL uses mode 3 by default (synchronous_commit = on, but WAL
 * is fsync'd once per commit, not per statement). TigerBeetle batches
 * 32 operations before a single fsync.
 */
import fs from 'node:fs';
import path from 'node:path';

export class WriteAheadLog {
  constructor(filepath, mode = 'wal_batch') {
    this.filepath = filepath;
    this.mode = mode; // 'no_wal' | 'wal_fsync' | 'wal_batch'
    this.fd = null;
    this.offset = 0;
    this.batchBuffer = [];
    this.batchSize = 32; // flush every N records (TigerBeetle uses 32)
    this.stats = { writes: 0, fsyncs: 0, bytesWritten: 0, batchFlushes: 0 };
  }

  open() {
    const dir = path.dirname(this.filepath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    if (this.mode === 'no_wal') return;
    this.fd = fs.openSync(this.filepath, 'w');
  }

  close() {
    if (this.mode !== 'no_wal' && this.fd !== null) {
      if (this.batchBuffer.length > 0) this._flushBatch();
      fs.fsyncSync(this.fd);
      fs.closeSync(this.fd);
      this.fd = null;
    }
  }

  /** Write a record to the WAL */
  appendRecord(record) {
    if (this.mode === 'no_wal') {
      this.stats.writes++;
      return;
    }

    // Encode: [length: u32] [checksum: u32] [data: bytes]
    const data = Buffer.isBuffer(record) ? record : Buffer.from(JSON.stringify(record));
    const header = Buffer.alloc(8);
    header.writeUInt32LE(data.length, 0);
    header.writeUInt32LE(this._checksum(data), 4);

    if (this.mode === 'wal_fsync') {
      // Write + fsync every record (safest, slowest)
      fs.writeSync(this.fd, header, 0, 8, this.offset);
      fs.writeSync(this.fd, data, 0, data.length, this.offset + 8);
      fs.fsyncSync(this.fd);
      this.offset += 8 + data.length;
      this.stats.writes++;
      this.stats.fsyncs++;
      this.stats.bytesWritten += 8 + data.length;
    } else if (this.mode === 'wal_batch') {
      // Buffer and flush in batches (group commit)
      this.batchBuffer.push({ header, data });
      this.stats.writes++;
      if (this.batchBuffer.length >= this.batchSize) {
        this._flushBatch();
      }
    }
  }

  _flushBatch() {
    if (this.batchBuffer.length === 0) return;

    // Write all buffered records, then single fsync
    for (const { header, data } of this.batchBuffer) {
      fs.writeSync(this.fd, header, 0, 8, this.offset);
      fs.writeSync(this.fd, data, 0, data.length, this.offset + 8);
      this.offset += 8 + data.length;
      this.stats.bytesWritten += 8 + data.length;
    }
    fs.fsyncSync(this.fd);
    this.stats.fsyncs++;
    this.stats.batchFlushes++;
    this.batchBuffer = [];
  }

  /** Force flush any pending batch */
  flush() {
    if (this.mode === 'wal_batch') this._flushBatch();
  }

  _checksum(data) {
    // Simple FNV-1a 32-bit hash (fast, not cryptographic)
    let hash = 0x811c9dc5;
    for (let i = 0; i < data.length; i++) {
      hash ^= data[i];
      hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
  }

  getStats() {
    return {
      mode: this.mode,
      ...this.stats,
      fsyncsPerWrite: this.stats.writes > 0
        ? (this.stats.fsyncs / this.stats.writes).toFixed(3)
        : 'N/A',
    };
  }
}
