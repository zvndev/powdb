/**
 * B-Tree Index Implementation
 * 
 * A minimal in-memory B+ tree for benchmarking. This lets us measure
 * the actual cost of index lookups vs sequential scans, and compare
 * our index traversal against SQLite's built-in B-tree.
 * 
 * Design choices (matching what real databases do):
 * - B+ tree: all values in leaf nodes, internal nodes are just routing
 * - Configurable order (fan-out) to test how branching factor affects perf
 * - Leaf nodes linked for range scans
 * - Returns { page, offset } pointers (like a real index → heap lookup)
 */

export class BPlusTree {
  constructor(order = 128) {
    this.order = order; // max keys per node
    this.root = new LeafNode(order);
    this.size = 0;
    this.height = 1;
    this.stats = { comparisons: 0, nodesVisited: 0, splits: 0 };
  }

  /** Insert a key → value mapping */
  insert(key, value) {
    const result = this.root.insert(key, value, this.stats);
    if (result) {
      // Root was split — create new root
      const newRoot = new InternalNode(this.order);
      newRoot.keys = [result.key];
      newRoot.children = [this.root, result.node];
      this.root = newRoot;
      this.height++;
      this.stats.splits++;
    }
    this.size++;
  }

  /** Point lookup — returns value or null */
  lookup(key) {
    this.stats.nodesVisited = 0;
    this.stats.comparisons = 0;
    return this.root.lookup(key, this.stats);
  }

  /** Range scan — returns all values where low <= key <= high */
  range(low, high) {
    const results = [];
    let leaf = this._findLeaf(low);
    
    while (leaf) {
      for (let i = 0; i < leaf.keys.length; i++) {
        if (leaf.keys[i] > high) return results;
        if (leaf.keys[i] >= low) results.push(leaf.values[i]);
      }
      leaf = leaf.next;
    }
    return results;
  }

  _findLeaf(key) {
    let node = this.root;
    while (node instanceof InternalNode) {
      let i = 0;
      while (i < node.keys.length && key >= node.keys[i]) i++;
      node = node.children[i];
    }
    return node;
  }

  getStats() {
    return {
      size: this.size,
      height: this.height,
      order: this.order,
      ...this.stats,
    };
  }

  resetStats() {
    this.stats = { comparisons: 0, nodesVisited: 0, splits: 0 };
  }
}

class LeafNode {
  constructor(order) {
    this.order = order;
    this.keys = [];
    this.values = [];
    this.next = null; // linked list for range scans
  }

  lookup(key, stats) {
    stats.nodesVisited++;
    // Binary search within leaf
    let lo = 0, hi = this.keys.length - 1;
    while (lo <= hi) {
      stats.comparisons++;
      const mid = (lo + hi) >>> 1;
      if (this.keys[mid] === key) return this.values[mid];
      if (this.keys[mid] < key) lo = mid + 1;
      else hi = mid - 1;
    }
    return null;
  }

  insert(key, value, stats) {
    // Find insertion position (binary search)
    let pos = 0;
    let lo = 0, hi = this.keys.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      if (this.keys[mid] < key) { lo = mid + 1; pos = lo; }
      else { hi = mid - 1; pos = mid; }
    }
    if (lo > hi) pos = lo;

    this.keys.splice(pos, 0, key);
    this.values.splice(pos, 0, value);

    // Split if overflowing
    if (this.keys.length > this.order) {
      return this._split(stats);
    }
    return null;
  }

  _split(stats) {
    stats.splits++;
    const mid = Math.floor(this.keys.length / 2);
    const newLeaf = new LeafNode(this.order);
    
    newLeaf.keys = this.keys.splice(mid);
    newLeaf.values = this.values.splice(mid);
    newLeaf.next = this.next;
    this.next = newLeaf;

    return { key: newLeaf.keys[0], node: newLeaf };
  }
}

class InternalNode {
  constructor(order) {
    this.order = order;
    this.keys = [];
    this.children = [];
  }

  lookup(key, stats) {
    stats.nodesVisited++;
    // Find correct child
    let i = 0;
    while (i < this.keys.length && key >= this.keys[i]) {
      stats.comparisons++;
      i++;
    }
    return this.children[i].lookup(key, stats);
  }

  insert(key, value, stats) {
    // Find correct child
    let i = 0;
    while (i < this.keys.length && key >= this.keys[i]) i++;
    
    const result = this.children[i].insert(key, value, stats);
    if (!result) return null;

    // Child was split — insert the new key + child pointer
    this.keys.splice(i, 0, result.key);
    this.children.splice(i + 1, 0, result.node);

    // Split this internal node if overflowing
    if (this.keys.length > this.order) {
      return this._split(stats);
    }
    return null;
  }

  _split(stats) {
    stats.splits++;
    const mid = Math.floor(this.keys.length / 2);
    const newNode = new InternalNode(this.order);
    const promoteKey = this.keys[mid];

    newNode.keys = this.keys.splice(mid + 1);
    newNode.children = this.children.splice(mid + 1);
    this.keys.pop(); // remove the promoted key

    return { key: promoteKey, node: newNode };
  }
}

/**
 * Hash index — for comparison against B-tree on point lookups.
 * Hash indexes are O(1) but can't do range scans.
 * PostgreSQL supports hash indexes but they're rarely used.
 */
export class HashIndex {
  constructor(buckets = 16384) {
    this.buckets = new Array(buckets).fill(null).map(() => []);
    this.bucketCount = buckets;
    this.size = 0;
    this.stats = { comparisons: 0 };
  }

  insert(key, value) {
    const bucket = this._hash(key);
    this.buckets[bucket].push({ key, value });
    this.size++;
  }

  lookup(key) {
    this.stats.comparisons = 0;
    const bucket = this._hash(key);
    const chain = this.buckets[bucket];
    for (let i = 0; i < chain.length; i++) {
      this.stats.comparisons++;
      if (chain[i].key === key) return chain[i].value;
    }
    return null;
  }

  _hash(key) {
    // Simple but effective integer hash (MurmurHash3-like finalizer)
    let h = key | 0;
    h = Math.imul(h ^ (h >>> 16), 0x85ebca6b);
    h = Math.imul(h ^ (h >>> 13), 0xc2b2ae35);
    h = (h ^ (h >>> 16)) >>> 0;
    return h % this.bucketCount;
  }

  getStats() {
    // Calculate bucket distribution
    let maxChain = 0, totalChain = 0, usedBuckets = 0;
    for (const b of this.buckets) {
      if (b.length > 0) {
        usedBuckets++;
        totalChain += b.length;
        if (b.length > maxChain) maxChain = b.length;
      }
    }
    return {
      size: this.size,
      buckets: this.bucketCount,
      usedBuckets,
      avgChainLength: usedBuckets > 0 ? (totalChain / usedBuckets).toFixed(1) : 0,
      maxChainLength: maxChain,
      ...this.stats,
    };
  }
}
