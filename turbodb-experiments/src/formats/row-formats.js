/**
 * Row Format Implementations
 * 
 * Each format stores the same logical data (id: int32, name: string, email: string, age: int16)
 * but with different physical layouts. This lets us measure:
 * - Storage overhead per row
 * - Write throughput
 * - Point read latency
 * - Scan throughput (reading one column vs all columns)
 */

// ============================================================
// FORMAT 1: PostgreSQL-style Heap
// 
// Mimics PostgreSQL's actual tuple format:
// - 24-byte page header
// - 4-byte line pointers (growing down from header)
// - Tuples (growing up from bottom of page)
// - Each tuple: 23-byte header + 1 byte padding + data
// 
// This is the "baseline" — it's what you're comparing against.
// ============================================================
export class PgHeapFormat {
  constructor(pageSize = 8192) {
    this.pageSize = pageSize;
    this.name = 'pg-heap';
    this.pages = [];
    this.totalRows = 0;
    this.totalDataBytes = 0;
    this.totalOverheadBytes = 0;
  }

  // Page layout constants (matching real PostgreSQL)
  static PAGE_HEADER_SIZE = 24;    // PageHeaderData
  static LINE_POINTER_SIZE = 4;    // ItemIdData
  static TUPLE_HEADER_SIZE = 23;   // HeapTupleHeaderData
  static TUPLE_ALIGNMENT = 8;     // MAXALIGN

  createPage() {
    const buf = Buffer.alloc(this.pageSize);
    // Page header
    buf.writeUInt32LE(0, 0);          // pd_lsn (low)
    buf.writeUInt32LE(0, 4);          // pd_lsn (high)
    buf.writeUInt16LE(0, 8);          // pd_checksum
    buf.writeUInt16LE(0, 10);         // pd_flags
    buf.writeUInt16LE(PgHeapFormat.PAGE_HEADER_SIZE, 12); // pd_lower (start of free space)
    buf.writeUInt16LE(this.pageSize, 14);                  // pd_upper (end of free space)
    buf.writeUInt16LE(0, 16);         // pd_special
    buf.writeUInt16LE(0, 18);         // pd_pagesize_version
    // bytes 20-23: padding
    this.pages.push(buf);
    return this.pages.length - 1;
  }

  insertRow(row) {
    // Encode the data portion
    const dataBytes = this._encodeRowData(row);
    
    // Total tuple size: 23-byte header + alignment padding + data
    const headerPlusPad = Math.ceil(PgHeapFormat.TUPLE_HEADER_SIZE / PgHeapFormat.TUPLE_ALIGNMENT) 
                          * PgHeapFormat.TUPLE_ALIGNMENT; // = 24
    const tupleSize = headerPlusPad + dataBytes.length;
    const linePointerCost = PgHeapFormat.LINE_POINTER_SIZE;
    
    // Find a page with enough space, or create one
    let pageIdx = this.pages.length - 1;
    if (pageIdx < 0 || !this._hasSpace(pageIdx, tupleSize + linePointerCost)) {
      pageIdx = this.createPage();
    }
    
    const page = this.pages[pageIdx];
    const pdLower = page.readUInt16LE(12);
    const pdUpper = page.readUInt16LE(14);
    
    // Write line pointer at pd_lower
    const tupleOffset = pdUpper - tupleSize;
    const lpValue = (tupleOffset & 0x7FFF) | ((tupleSize & 0x7FFF) << 15) | (1 << 30); // offset + length + normal flag
    page.writeUInt32LE(lpValue, pdLower);
    
    // Write tuple header at tupleOffset
    page.writeUInt32LE(this.totalRows + 1, tupleOffset);     // t_xmin (fake txid)
    page.writeUInt32LE(0, tupleOffset + 4);                   // t_xmax
    page.writeUInt32LE(0, tupleOffset + 8);                   // t_cid
    // t_ctid: 6 bytes (block + offset)
    page.writeUInt32LE(pageIdx, tupleOffset + 12);            // block
    page.writeUInt16LE(0, tupleOffset + 16);                  // offset
    page.writeUInt16LE(0x0902, tupleOffset + 18);             // t_infomask
    page.writeUInt16LE(4, tupleOffset + 20);                  // t_infomask2 (4 columns)
    page.writeUInt8(headerPlusPad, tupleOffset + 22);         // t_hoff
    
    // Write data after header + padding
    dataBytes.copy(page, tupleOffset + headerPlusPad);
    
    // Update page header
    page.writeUInt16LE(pdLower + linePointerCost, 12);  // advance pd_lower
    page.writeUInt16LE(tupleOffset, 14);                 // retreat pd_upper
    
    this.totalRows++;
    this.totalDataBytes += dataBytes.length;
    this.totalOverheadBytes += headerPlusPad + linePointerCost; // 24 + 4 = 28 bytes overhead
    
    return { page: pageIdx, offset: tupleOffset };
  }

  readRow(pageIdx, tupleOffset) {
    const page = this.pages[pageIdx];
    const tHoff = page.readUInt8(tupleOffset + 22);
    return this._decodeRowData(page, tupleOffset + tHoff);
  }

  scanAllRows(columnName = null) {
    const results = [];
    for (let p = 0; p < this.pages.length; p++) {
      const page = this.pages[p];
      const pdLower = page.readUInt16LE(12);
      const numPointers = (pdLower - PgHeapFormat.PAGE_HEADER_SIZE) / PgHeapFormat.LINE_POINTER_SIZE;
      
      for (let i = 0; i < numPointers; i++) {
        const lpOffset = PgHeapFormat.PAGE_HEADER_SIZE + i * PgHeapFormat.LINE_POINTER_SIZE;
        const lpValue = page.readUInt32LE(lpOffset);
        const tupleOffset = lpValue & 0x7FFF;
        const tHoff = page.readUInt8(tupleOffset + 22);
        const row = this._decodeRowData(page, tupleOffset + tHoff);
        
        if (columnName) {
          results.push(row[columnName]);
        } else {
          results.push(row);
        }
      }
    }
    return results;
  }

  _hasSpace(pageIdx, needed) {
    const page = this.pages[pageIdx];
    const pdLower = page.readUInt16LE(12);
    const pdUpper = page.readUInt16LE(14);
    return (pdUpper - pdLower) >= needed;
  }

  _encodeRowData(row) {
    // Fixed layout: id (4 bytes) + age (2 bytes) + name (varlena) + email (varlena)
    const nameBuf = Buffer.from(row.name, 'utf8');
    const emailBuf = Buffer.from(row.email, 'utf8');
    const totalLen = 4 + 2 + 4 + nameBuf.length + 4 + emailBuf.length;
    const buf = Buffer.alloc(totalLen);
    let off = 0;
    buf.writeInt32LE(row.id, off); off += 4;
    buf.writeInt16LE(row.age, off); off += 2;
    // varlena: 4-byte length prefix (PostgreSQL style)
    buf.writeInt32LE(nameBuf.length + 4, off); off += 4;
    nameBuf.copy(buf, off); off += nameBuf.length;
    buf.writeInt32LE(emailBuf.length + 4, off); off += 4;
    emailBuf.copy(buf, off);
    return buf;
  }

  _decodeRowData(page, dataOffset) {
    let off = dataOffset;
    const id = page.readInt32LE(off); off += 4;
    const age = page.readInt16LE(off); off += 2;
    const nameLen = page.readInt32LE(off) - 4; off += 4;
    const name = page.subarray(off, off + nameLen).toString('utf8'); off += nameLen;
    const emailLen = page.readInt32LE(off) - 4; off += 4;
    const email = page.subarray(off, off + emailLen).toString('utf8');
    return { id, name, email, age };
  }

  getStorageStats() {
    const totalBytes = this.pages.length * this.pageSize;
    const pageHeaderBytes = this.pages.length * PgHeapFormat.PAGE_HEADER_SIZE;
    // Free space inside pages
    let freeSpace = 0;
    for (const page of this.pages) {
      const pdLower = page.readUInt16LE(12);
      const pdUpper = page.readUInt16LE(14);
      freeSpace += (pdUpper - pdLower);
    }
    return {
      format: this.name,
      rows: this.totalRows,
      pages: this.pages.length,
      totalBytes,
      dataBytes: this.totalDataBytes,
      overheadBytes: this.totalOverheadBytes,
      pageHeaderBytes,
      freeSpaceBytes: freeSpace,
      bytesPerRow: this.totalRows > 0 ? Math.round(totalBytes / this.totalRows) : 0,
      overheadPerRow: this.totalRows > 0 ? Math.round(this.totalOverheadBytes / this.totalRows) : 0,
      overheadPct: this.totalRows > 0 
        ? Math.round(this.totalOverheadBytes / (this.totalDataBytes + this.totalOverheadBytes) * 100) 
        : 0,
    };
  }
}


// ============================================================
// FORMAT 2: Compact Row Store
// 
// Minimal overhead design:
// - 8-byte page header (just a row count + free offset)
// - No line pointers (rows are packed contiguously)
// - 2-byte row length prefix (no 23-byte tuple header)
// - Varint-encoded string lengths
// 
// This is what you'd build if you started fresh.
// ============================================================
export class CompactRowFormat {
  constructor(pageSize = 4096) {
    this.pageSize = pageSize;
    this.name = 'compact-row';
    this.pages = [];
    this.totalRows = 0;
    this.totalDataBytes = 0;
    this.totalOverheadBytes = 0;
  }

  static PAGE_HEADER_SIZE = 8; // row_count (u16) + data_end_offset (u16) + checksum (u32)

  createPage() {
    const buf = Buffer.alloc(this.pageSize);
    buf.writeUInt16LE(0, 0);  // row count
    buf.writeUInt16LE(CompactRowFormat.PAGE_HEADER_SIZE, 2);  // data end offset
    buf.writeUInt32LE(0, 4);  // checksum placeholder
    this.pages.push(buf);
    return this.pages.length - 1;
  }

  insertRow(row) {
    const encoded = this._encodeRow(row);
    // 2-byte length prefix + encoded data
    const totalSize = 2 + encoded.length;
    
    let pageIdx = this.pages.length - 1;
    if (pageIdx < 0 || !this._hasSpace(pageIdx, totalSize)) {
      pageIdx = this.createPage();
    }
    
    const page = this.pages[pageIdx];
    const rowCount = page.readUInt16LE(0);
    const dataEnd = page.readUInt16LE(2);
    
    // Write length prefix + data contiguously
    page.writeUInt16LE(encoded.length, dataEnd);
    encoded.copy(page, dataEnd + 2);
    
    page.writeUInt16LE(rowCount + 1, 0);
    page.writeUInt16LE(dataEnd + totalSize, 2);
    
    this.totalRows++;
    this.totalDataBytes += encoded.length;
    this.totalOverheadBytes += 2; // just the length prefix
    
    return { page: pageIdx, offset: dataEnd };
  }

  readRow(pageIdx, offset) {
    const page = this.pages[pageIdx];
    const len = page.readUInt16LE(offset);
    return this._decodeRow(page, offset + 2, len);
  }

  scanAllRows(columnName = null) {
    const results = [];
    for (const page of this.pages) {
      const rowCount = page.readUInt16LE(0);
      let off = CompactRowFormat.PAGE_HEADER_SIZE;
      
      for (let i = 0; i < rowCount; i++) {
        const len = page.readUInt16LE(off);
        const row = this._decodeRow(page, off + 2, len);
        if (columnName) {
          results.push(row[columnName]);
        } else {
          results.push(row);
        }
        off += 2 + len;
      }
    }
    return results;
  }

  _hasSpace(pageIdx, needed) {
    const page = this.pages[pageIdx];
    const dataEnd = page.readUInt16LE(2);
    return (this.pageSize - dataEnd) >= needed;
  }

  _encodeRow(row) {
    // Pack tightly: id (4) + age (2) + name_len (varint) + name + email_len (varint) + email
    const nameBuf = Buffer.from(row.name, 'utf8');
    const emailBuf = Buffer.from(row.email, 'utf8');
    // Use 1-byte length for strings under 128 bytes (varint-style)
    const buf = Buffer.alloc(4 + 2 + 1 + nameBuf.length + 1 + emailBuf.length);
    let off = 0;
    buf.writeInt32LE(row.id, off); off += 4;
    buf.writeInt16LE(row.age, off); off += 2;
    buf.writeUInt8(nameBuf.length, off); off += 1;
    nameBuf.copy(buf, off); off += nameBuf.length;
    buf.writeUInt8(emailBuf.length, off); off += 1;
    emailBuf.copy(buf, off);
    return buf;
  }

  _decodeRow(page, offset, len) {
    let off = offset;
    const id = page.readInt32LE(off); off += 4;
    const age = page.readInt16LE(off); off += 2;
    const nameLen = page.readUInt8(off); off += 1;
    const name = page.subarray(off, off + nameLen).toString('utf8'); off += nameLen;
    const emailLen = page.readUInt8(off); off += 1;
    const email = page.subarray(off, off + emailLen).toString('utf8');
    return { id, name, email, age };
  }

  getStorageStats() {
    const totalBytes = this.pages.length * this.pageSize;
    const pageHeaderBytes = this.pages.length * CompactRowFormat.PAGE_HEADER_SIZE;
    let freeSpace = 0;
    for (const page of this.pages) {
      freeSpace += this.pageSize - page.readUInt16LE(2);
    }
    return {
      format: this.name,
      rows: this.totalRows,
      pages: this.pages.length,
      totalBytes,
      dataBytes: this.totalDataBytes,
      overheadBytes: this.totalOverheadBytes,
      pageHeaderBytes,
      freeSpaceBytes: freeSpace,
      bytesPerRow: this.totalRows > 0 ? Math.round(totalBytes / this.totalRows) : 0,
      overheadPerRow: this.totalRows > 0 ? Math.round(this.totalOverheadBytes / this.totalRows) : 0,
      overheadPct: this.totalRows > 0 
        ? Math.round(this.totalOverheadBytes / (this.totalDataBytes + this.totalOverheadBytes) * 100) 
        : 0,
    };
  }
}


// ============================================================
// FORMAT 3: Columnar Store
// 
// Each column stored separately in its own page sequence.
// This is what DuckDB and ClickHouse do.
// - Fixed-width columns packed contiguously (id, age)
// - Variable-width columns use offset arrays + data blocks
// - Reads of a single column touch minimal data
// ============================================================
export class ColumnarFormat {
  constructor(pageSize = 4096) {
    this.pageSize = pageSize;
    this.name = 'columnar';
    this.totalRows = 0;
    
    // Each column gets its own buffer
    this.columns = {
      id:    { type: 'int32', width: 4, data: [] },
      age:   { type: 'int16', width: 2, data: [] },
      name:  { type: 'string', data: [], offsets: [] },
      email: { type: 'string', data: [], offsets: [] },
    };
    
    // String data storage
    this.stringBuffers = { name: [], email: [] };
    this.totalDataBytes = 0;
    this.totalOverheadBytes = 0;
  }

  insertRow(row) {
    this.columns.id.data.push(row.id);
    this.columns.age.data.push(row.age);
    this.columns.name.data.push(row.name);
    this.columns.email.data.push(row.email);
    this.totalRows++;
    
    const nameBytes = Buffer.byteLength(row.name, 'utf8');
    const emailBytes = Buffer.byteLength(row.email, 'utf8');
    this.totalDataBytes += 4 + 2 + nameBytes + emailBytes;
    this.totalOverheadBytes += 8; // offset entries for strings (4 bytes each)
    
    return { row: this.totalRows - 1 };
  }

  readRow(rowIdx) {
    return {
      id: this.columns.id.data[rowIdx],
      age: this.columns.age.data[rowIdx],
      name: this.columns.name.data[rowIdx],
      email: this.columns.email.data[rowIdx],
    };
  }

  /** Read a single column — this is where columnar shines */
  readColumn(columnName) {
    return this.columns[columnName].data;
  }

  scanAllRows(columnName = null) {
    if (columnName) {
      return [...this.columns[columnName].data];
    }
    const results = [];
    for (let i = 0; i < this.totalRows; i++) {
      results.push(this.readRow(i));
    }
    return results;
  }

  /** Materialize to pages for fair storage comparison */
  materializeToPages() {
    // Pack each column into pages
    let totalPages = 0;
    
    // Fixed-width columns: values packed contiguously
    const idPages = Math.ceil((this.totalRows * 4) / this.pageSize);
    const agePages = Math.ceil((this.totalRows * 2) / this.pageSize);
    
    // String columns: offset array + data
    let nameTotalBytes = 0;
    let emailTotalBytes = 0;
    for (let i = 0; i < this.totalRows; i++) {
      nameTotalBytes += Buffer.byteLength(this.columns.name.data[i], 'utf8');
      emailTotalBytes += Buffer.byteLength(this.columns.email.data[i], 'utf8');
    }
    const namePages = Math.ceil((this.totalRows * 4 + nameTotalBytes) / this.pageSize);
    const emailPages = Math.ceil((this.totalRows * 4 + emailTotalBytes) / this.pageSize);
    
    totalPages = idPages + agePages + namePages + emailPages;
    return totalPages;
  }

  getStorageStats() {
    const totalPages = this.materializeToPages();
    const totalBytes = totalPages * this.pageSize;
    return {
      format: this.name,
      rows: this.totalRows,
      pages: totalPages,
      totalBytes,
      dataBytes: this.totalDataBytes,
      overheadBytes: this.totalOverheadBytes,
      pageHeaderBytes: 0, // no per-page headers in this simplified version
      freeSpaceBytes: totalBytes - this.totalDataBytes - this.totalOverheadBytes,
      bytesPerRow: this.totalRows > 0 ? Math.round(totalBytes / this.totalRows) : 0,
      overheadPerRow: this.totalRows > 0 ? Math.round(this.totalOverheadBytes / this.totalRows) : 0,
      overheadPct: this.totalRows > 0 
        ? Math.round(this.totalOverheadBytes / (this.totalDataBytes + this.totalOverheadBytes) * 100) 
        : 0,
    };
  }
}
