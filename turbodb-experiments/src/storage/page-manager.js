import fs from 'node:fs';
import path from 'node:path';

/**
 * PageManager - raw page-level I/O
 * 
 * This is the absolute bottom of the stack: read and write fixed-size
 * pages to a file. Every database does this. The questions are:
 * - What page size is fastest on this hardware?
 * - How much does OS caching help vs hurt?
 * - What's the raw I/O throughput we're working with?
 */
export class PageManager {
  constructor(filepath, pageSize = 4096) {
    this.filepath = filepath;
    this.pageSize = pageSize;
    this.fd = null;
    this.pageCount = 0;
    this.stats = { reads: 0, writes: 0, bytesRead: 0, bytesWritten: 0 };
  }

  open() {
    const dir = path.dirname(this.filepath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    // O_RDWR | O_CREAT — standard flags
    // Note: O_DIRECT requires aligned buffers and isn't available in Node.js
    // without native addons. For this experiment we use standard I/O and
    // measure with OS cache cleared between runs for honest numbers.
    this.fd = fs.openSync(this.filepath, 'w+');
    const stat = fs.fstatSync(this.fd);
    this.pageCount = Math.floor(stat.size / this.pageSize);
  }

  close() {
    if (this.fd !== null) {
      fs.fsyncSync(this.fd);
      fs.closeSync(this.fd);
      this.fd = null;
    }
  }

  /** Allocate a new page, returns page number */
  allocPage() {
    const pageNum = this.pageCount++;
    const buf = Buffer.alloc(this.pageSize);
    this.writePage(pageNum, buf);
    return pageNum;
  }

  /** Write a full page to disk */
  writePage(pageNum, buffer) {
    if (buffer.length !== this.pageSize) {
      throw new Error(`Buffer size ${buffer.length} != page size ${this.pageSize}`);
    }
    const offset = pageNum * this.pageSize;
    fs.writeSync(this.fd, buffer, 0, this.pageSize, offset);
    this.stats.writes++;
    this.stats.bytesWritten += this.pageSize;
  }

  /** Read a full page from disk */
  readPage(pageNum) {
    const buf = Buffer.alloc(this.pageSize);
    const offset = pageNum * this.pageSize;
    fs.readSync(this.fd, buf, 0, this.pageSize, offset);
    this.stats.reads++;
    this.stats.bytesRead += this.pageSize;
    return buf;
  }

  /** Sync all pending writes to disk */
  sync() {
    fs.fsyncSync(this.fd);
  }

  /** Drop OS page cache for this file (Linux only, needs sudo) */
  dropCache() {
    try {
      fs.writeFileSync('/proc/sys/vm/drop_caches', '3');
    } catch {
      // Not running as root or not Linux — that's fine for relative comparisons
    }
  }

  getStats() {
    return { ...this.stats, pageCount: this.pageCount, pageSize: this.pageSize };
  }

  resetStats() {
    this.stats = { reads: 0, writes: 0, bytesRead: 0, bytesWritten: 0 };
  }
}

/**
 * Simple buffer pool — keeps hot pages in memory
 * This is a stripped-down version of what PostgreSQL's shared_buffers does.
 * Uses LRU eviction (clock-sweep would be more realistic but LRU is clearer for experiments).
 */
export class BufferPool {
  constructor(pageManager, maxPages = 1024) {
    this.pm = pageManager;
    this.maxPages = maxPages;
    this.cache = new Map(); // pageNum → { buffer, dirty, lastAccess }
    this.accessCounter = 0;
    this.stats = { hits: 0, misses: 0, evictions: 0 };
  }

  getPage(pageNum) {
    this.accessCounter++;
    if (this.cache.has(pageNum)) {
      this.stats.hits++;
      const entry = this.cache.get(pageNum);
      entry.lastAccess = this.accessCounter;
      return entry.buffer;
    }

    this.stats.misses++;
    if (this.cache.size >= this.maxPages) {
      this._evict();
    }

    const buffer = this.pm.readPage(pageNum);
    this.cache.set(pageNum, { buffer, dirty: false, lastAccess: this.accessCounter });
    return buffer;
  }

  markDirty(pageNum) {
    const entry = this.cache.get(pageNum);
    if (entry) entry.dirty = true;
  }

  flush() {
    for (const [pageNum, entry] of this.cache) {
      if (entry.dirty) {
        this.pm.writePage(pageNum, entry.buffer);
        entry.dirty = false;
      }
    }
    this.pm.sync();
  }

  _evict() {
    let oldestPage = null;
    let oldestAccess = Infinity;
    for (const [pageNum, entry] of this.cache) {
      if (entry.lastAccess < oldestAccess) {
        oldestAccess = entry.lastAccess;
        oldestPage = pageNum;
      }
    }
    if (oldestPage !== null) {
      const entry = this.cache.get(oldestPage);
      if (entry.dirty) {
        this.pm.writePage(oldestPage, entry.buffer);
      }
      this.cache.delete(oldestPage);
      this.stats.evictions++;
    }
  }

  getStats() {
    const total = this.stats.hits + this.stats.misses;
    return {
      ...this.stats,
      hitRate: total > 0 ? (this.stats.hits / total * 100).toFixed(1) + '%' : 'N/A',
      cached: this.cache.size,
      ioStats: this.pm.getStats()
    };
  }
}
