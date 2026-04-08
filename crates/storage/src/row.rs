use crate::types::*;

/// Encode a row of values into the compact binary format.
///
/// Layout: [length: u16] [null_bitmap] [fixed columns packed] [var offset table] [var data]
///
/// Fixed columns are written in schema order, with placeholder zeros for Empty values.
/// Variable columns use an offset table (n_var + 1 entries) pointing into var data.
/// Overhead: 2 bytes (length) + ceil(n_cols/8) bytes (bitmap).
///
/// Mission C Phase 2: kept as a thin wrapper around [`encode_row_into`] so
/// existing tests continue to work. Hot callers (bench insert/update loops)
/// should go through `encode_row_into` and reuse the output buffer.
pub fn encode_row(schema: &Schema, values: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_row_into(schema, values, &mut out);
    out
}

/// Encode a row into a caller-provided scratch buffer.
///
/// Mission C Phase 2: the previous `encode_row` allocated 5-6 temporary Vecs
/// per call (null bitmap, fixed buf, var indices, var data, var offsets,
/// final buf). On the `update_by_filter` bench that fired ~50K times. The
/// rewrite below walks the schema twice and writes straight into `out`,
/// reusing the buffer's backing store between calls.
///
/// Contract:
/// - `out` is cleared and filled with exactly the encoded row bytes.
/// - No allocations happen if `out.capacity()` is already large enough
///   (the common case after the first insert of a given shape).
pub fn encode_row_into(schema: &Schema, values: &[Value], out: &mut Vec<u8>) {
    debug_assert_eq!(values.len(), schema.columns.len());

    out.clear();

    let n_cols = schema.columns.len();
    let bitmap_size = (n_cols + 7) / 8;

    // First pass: compute sizes so we can reserve once and avoid any
    // intermediate growth. The pass walks the same value slice twice, but
    // the second pass writes without branching on capacity.
    let mut fixed_region_size = 0usize;
    let mut n_var = 0usize;
    let mut var_data_size = 0usize;
    for (i, col) in schema.columns.iter().enumerate() {
        if is_fixed_size(col.type_id) {
            fixed_region_size += fixed_size(col.type_id).unwrap();
        } else {
            n_var += 1;
            if !values[i].is_empty() {
                match &values[i] {
                    Value::Str(s) => var_data_size += s.len(),
                    Value::Bytes(b) => var_data_size += b.len(),
                    _ => {}
                }
            }
        }
    }

    let n_offsets = n_var + 1;
    let body_size = bitmap_size + fixed_region_size + n_offsets * 2 + var_data_size;
    let total_size = 2 + body_size;

    out.reserve(total_size);

    // Length prefix — placeholder that we'll fill in after writing. The
    // total is already known, so just write it directly.
    out.extend_from_slice(&(total_size as u16).to_le_bytes());

    // Null bitmap — write byte at a time.
    let bitmap_start = out.len();
    out.resize(bitmap_start + bitmap_size, 0);
    for (i, val) in values.iter().enumerate() {
        if val.is_empty() {
            out[bitmap_start + i / 8] |= 1 << (i % 8);
        }
    }

    // Fixed columns packed in schema order.
    for (i, col) in schema.columns.iter().enumerate() {
        if !is_fixed_size(col.type_id) {
            continue;
        }
        let sz = fixed_size(col.type_id).unwrap();
        if values[i].is_empty() {
            // Placeholder zeros so offsets stay predictable.
            out.resize(out.len() + sz, 0);
        } else {
            match &values[i] {
                Value::Int(v)      => out.extend_from_slice(&v.to_le_bytes()),
                Value::Float(v)    => out.extend_from_slice(&v.to_le_bytes()),
                Value::Bool(v)     => out.push(if *v { 1 } else { 0 }),
                Value::DateTime(v) => out.extend_from_slice(&v.to_le_bytes()),
                Value::Uuid(v)     => out.extend_from_slice(v),
                _ => unreachable!("fixed column with non-fixed value"),
            }
        }
    }

    // Variable-column offset table. Compute as we go.
    let offsets_start = out.len();
    out.resize(offsets_start + n_offsets * 2, 0);

    let mut var_cursor: u16 = 0;
    let mut off_slot = 0usize;
    for (i, col) in schema.columns.iter().enumerate() {
        if is_fixed_size(col.type_id) {
            continue;
        }
        let pos = offsets_start + off_slot * 2;
        out[pos..pos + 2].copy_from_slice(&var_cursor.to_le_bytes());
        off_slot += 1;
        match &values[i] {
            Value::Str(s) => var_cursor += s.len() as u16,
            Value::Bytes(b) => var_cursor += b.len() as u16,
            _ => {}
        }
    }
    // End sentinel.
    let end_pos = offsets_start + off_slot * 2;
    out[end_pos..end_pos + 2].copy_from_slice(&var_cursor.to_le_bytes());

    // Variable-column data.
    for (i, col) in schema.columns.iter().enumerate() {
        if is_fixed_size(col.type_id) {
            continue;
        }
        match &values[i] {
            Value::Str(s) => out.extend_from_slice(s.as_bytes()),
            Value::Bytes(b) => out.extend_from_slice(b),
            Value::Empty => {} // zero-length
            _ => unreachable!("variable column with non-variable value"),
        }
    }

    debug_assert_eq!(out.len(), total_size);
}

/// Precomputed layout information for fast selective column decoding.
///
/// Computing offsets requires iterating through schema columns every time,
/// which is wasteful when decoding thousands of rows. This struct caches the
/// layout once so that `decode_column` can jump directly to the right byte
/// offset.
pub struct RowLayout {
    /// Byte offset within the fixed-column region for each fixed column.
    /// Variable-length columns have `None`.
    fixed_offsets: Vec<Option<usize>>,
    /// Total size of the fixed-column region in bytes.
    fixed_region_size: usize,
    /// For each column: if it is variable-length, its index within the
    /// variable-column offset table. Fixed columns have `None`.
    var_index: Vec<Option<usize>>,
    /// Total number of variable-length columns.
    n_var: usize,
    /// Size of the null bitmap in bytes.
    bitmap_size: usize,
}

impl RowLayout {
    /// Fixed byte offset for a column (None if variable-length).
    #[inline(always)]
    pub fn fixed_offset(&self, col_idx: usize) -> Option<usize> {
        self.fixed_offsets[col_idx]
    }

    /// Size of the null bitmap in bytes.
    #[inline(always)]
    pub fn bitmap_size(&self) -> usize {
        self.bitmap_size
    }

    /// Build a `RowLayout` from a schema. This is cheap — do it once per scan,
    /// not once per row.
    pub fn new(schema: &Schema) -> Self {
        let n_cols = schema.columns.len();
        let bitmap_size = (n_cols + 7) / 8;

        let mut fixed_offsets = vec![None; n_cols];
        let mut var_index = vec![None; n_cols];
        let mut fixed_pos: usize = 0;
        let mut var_count: usize = 0;

        for (i, col) in schema.columns.iter().enumerate() {
            if is_fixed_size(col.type_id) {
                fixed_offsets[i] = Some(fixed_pos);
                fixed_pos += fixed_size(col.type_id).unwrap();
            } else {
                var_index[i] = Some(var_count);
                var_count += 1;
            }
        }

        RowLayout {
            fixed_offsets,
            fixed_region_size: fixed_pos,
            var_index,
            n_var: var_count,
            bitmap_size,
        }
    }
}

/// Decode a single column from the raw row bytes without allocating anything
/// for other columns.
///
/// Mission F: marked `#[inline]` so the compiler can specialise it inside
/// the per-row scan loops in `executor::project_filter_limit_fast`. With LTO
/// on, this allows the type-id match to fold away when the caller knows the
/// column type.
#[inline]
pub fn decode_column(schema: &Schema, layout: &RowLayout, data: &[u8], col_idx: usize) -> Value {
    let col = &schema.columns[col_idx];

    // Check null bitmap
    let bitmap_start = 2; // skip 2-byte length prefix
    let is_null = (data[bitmap_start + col_idx / 8] >> (col_idx % 8)) & 1 == 1;
    if is_null {
        return Value::Empty;
    }

    let fixed_start = 2 + layout.bitmap_size;

    if let Some(offset) = layout.fixed_offsets[col_idx] {
        let pos = fixed_start + offset;
        match col.type_id {
            TypeId::Int => {
                Value::Int(i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()))
            }
            TypeId::Float => {
                Value::Float(f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()))
            }
            TypeId::Bool => {
                Value::Bool(data[pos] != 0)
            }
            TypeId::DateTime => {
                Value::DateTime(i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()))
            }
            TypeId::Uuid => {
                let mut v = [0u8; 16];
                v.copy_from_slice(&data[pos..pos + 16]);
                Value::Uuid(v)
            }
            _ => unreachable!(),
        }
    } else {
        let vi = layout.var_index[col_idx].unwrap();
        let offset_table_start = fixed_start + layout.fixed_region_size;
        let off_pos = offset_table_start + vi * 2;
        let next_off_pos = offset_table_start + (vi + 1) * 2;
        let var_offset = u16::from_le_bytes(data[off_pos..off_pos + 2].try_into().unwrap()) as usize;
        let var_next = u16::from_le_bytes(data[next_off_pos..next_off_pos + 2].try_into().unwrap()) as usize;

        let var_data_start = offset_table_start + (layout.n_var + 1) * 2;
        let start = var_data_start + var_offset;
        let end = var_data_start + var_next;
        let bytes = &data[start..end];

        match col.type_id {
            TypeId::Str => Value::Str(String::from_utf8_lossy(bytes).into_owned()),
            TypeId::Bytes => Value::Bytes(bytes.to_vec()),
            _ => unreachable!(),
        }
    }
}

/// Patch a single variable-length column in-place inside an already-encoded
/// row's raw bytes, shrinking the row if the new value is smaller than the
/// old one. Returns the new total row length on success, or `None` if the
/// new value would grow the row (caller must fall back to the full re-encode
/// path).
///
/// Mission C Phase 10: `update_by_filter` on the Mission A bench changes
/// `status` from one of `"active"/"inactive"/"pending"` (6-8 bytes) to
/// `"senior"` (6 bytes) for ~50K matching rows per iteration. Every single
/// row shrinks or matches — the old slow path still paid for a full
/// `decode_row` (3 String allocations per row) and `encode_row_into` (fresh
/// bitmap + fixed region + offset table walk) on every call. This helper
/// does the whole patch with 0 allocations by:
///   1. reading the old var offset pair from the offset table,
///   2. writing the new bytes directly over the old ones,
///   3. shifting any trailing var data back by `delta`,
///   4. decrementing every offset after the patched column by `delta`,
///   5. clearing the null bit (or setting it, if the new value is `None`),
///   6. rewriting the 2-byte length prefix.
///
/// Assumes `col_idx` is a variable-length column. The caller is expected to
/// check this (via `layout.var_index[col_idx]`) before calling; a panic in
/// the `unwrap` path is a caller bug.
#[inline]
pub fn patch_var_column_in_place(
    bytes: &mut [u8],
    layout: &RowLayout,
    col_idx: usize,
    new_value: Option<&[u8]>,
) -> Option<u16> {
    let var_idx = layout.var_index[col_idx].expect("not a var column");
    let n_var = layout.n_var;

    let offset_table_start = 2 + layout.bitmap_size + layout.fixed_region_size;
    let var_data_start = offset_table_start + (n_var + 1) * 2;

    // Read old offsets for this var column from the offset table.
    let off_pos = offset_table_start + var_idx * 2;
    let next_off_pos = offset_table_start + (var_idx + 1) * 2;
    let old_var_offset =
        u16::from_le_bytes(bytes[off_pos..off_pos + 2].try_into().unwrap()) as usize;
    let old_var_next =
        u16::from_le_bytes(bytes[next_off_pos..next_off_pos + 2].try_into().unwrap()) as usize;
    let old_var_len = old_var_next - old_var_offset;

    let new_var_len = new_value.map(|v| v.len()).unwrap_or(0);
    if new_var_len > old_var_len {
        return None; // grow path — let the caller fall back to re-encode
    }
    let delta = old_var_len - new_var_len;

    // Absolute byte positions inside the row.
    let old_var_abs_start = var_data_start + old_var_offset;
    let old_var_abs_end = var_data_start + old_var_next;
    let old_row_len = bytes.len();

    // Write new bytes (if any) over the old payload.
    if let Some(v) = new_value {
        bytes[old_var_abs_start..old_var_abs_start + new_var_len].copy_from_slice(v);
    }

    // Shift trailing var data back by `delta` (no-op when same-size).
    if delta > 0 {
        bytes.copy_within(
            old_var_abs_end..old_row_len,
            old_var_abs_start + new_var_len,
        );

        // Decrement every offset AFTER this var column. The entry at
        // var_idx stays the same (it's the start of our patched column);
        // entries var_idx+1..=n_var slide back by `delta`.
        for vi in (var_idx + 1)..=n_var {
            let pos = offset_table_start + vi * 2;
            let old_off = u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap());
            let new_off = old_off - delta as u16;
            bytes[pos..pos + 2].copy_from_slice(&new_off.to_le_bytes());
        }
    }

    // Null bitmap: clear or set the bit depending on new value.
    let bitmap_byte = 2 + col_idx / 8;
    let bit_mask = 1u8 << (col_idx % 8);
    if new_value.is_none() {
        bytes[bitmap_byte] |= bit_mask;
    } else {
        bytes[bitmap_byte] &= !bit_mask;
    }

    // Update the 2-byte length prefix.
    let new_row_len = old_row_len - delta;
    bytes[0..2].copy_from_slice(&(new_row_len as u16).to_le_bytes());

    Some(new_row_len as u16)
}

/// Decode a row from its compact binary format back into Values.
///
/// Mission F: `#[inline]` (not `always` — function is large) so LTO can fold
/// it into Filter+SeqScan when the inliner decides it's worth it.
#[inline]
pub fn decode_row(schema: &Schema, data: &[u8]) -> Row {
    let n_cols = schema.columns.len();
    let bitmap_size = (n_cols + 7) / 8;

    let mut pos = 2; // skip length prefix

    // Read null bitmap
    let null_bitmap = &data[pos..pos + bitmap_size];
    pos += bitmap_size;

    // We'll build the result in two passes: fixed first, then merge in variable
    let mut values = vec![Value::Empty; n_cols];

    // Read fixed-size columns
    for (i, col) in schema.columns.iter().enumerate() {
        if !is_fixed_size(col.type_id) {
            continue;
        }
        let is_null = (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
        let sz = fixed_size(col.type_id).unwrap();

        if is_null {
            pos += sz; // skip placeholder
            // values[i] is already Empty
        } else {
            values[i] = match col.type_id {
                TypeId::Int => {
                    let v = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    Value::Int(v)
                }
                TypeId::Float => {
                    let v = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    Value::Float(v)
                }
                TypeId::Bool => {
                    Value::Bool(data[pos] != 0)
                }
                TypeId::DateTime => {
                    let v = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    Value::DateTime(v)
                }
                TypeId::Uuid => {
                    let mut v = [0u8; 16];
                    v.copy_from_slice(&data[pos..pos + 16]);
                    Value::Uuid(v)
                }
                _ => unreachable!(),
            };
            pos += sz;
        }
    }

    // Read variable-length columns
    let var_col_indices: Vec<usize> = schema.columns.iter().enumerate()
        .filter(|(_, c)| !is_fixed_size(c.type_id))
        .map(|(i, _)| i)
        .collect();

    let n_var = var_col_indices.len();
    let n_offsets = n_var + 1;

    let mut var_offsets = Vec::with_capacity(n_offsets);
    for _ in 0..n_offsets {
        let off = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
        var_offsets.push(off as usize);
        pos += 2;
    }

    let var_data_start = pos;

    for (vi, &col_idx) in var_col_indices.iter().enumerate() {
        let is_null = (null_bitmap[col_idx / 8] >> (col_idx % 8)) & 1 == 1;
        if is_null {
            // values[col_idx] is already Empty
            continue;
        }
        let start = var_data_start + var_offsets[vi];
        let end = var_data_start + var_offsets[vi + 1];
        let bytes = &data[start..end];
        values[col_idx] = match schema.columns[col_idx].type_id {
            TypeId::Str => Value::Str(String::from_utf8_lossy(bytes).into_owned()),
            TypeId::Bytes => Value::Bytes(bytes.to_vec()),
            _ => unreachable!(),
        };
    }

    values
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_schema() -> Schema {
        Schema {
            table_name: "users".into(),
            columns: vec![
                ColumnDef { name: "name".into(),  type_id: TypeId::Str,  required: true,  position: 0 },
                ColumnDef { name: "email".into(), type_id: TypeId::Str,  required: true,  position: 1 },
                ColumnDef { name: "age".into(),   type_id: TypeId::Int,  required: false, position: 2 },
                ColumnDef { name: "active".into(),type_id: TypeId::Bool, required: true,  position: 3 },
            ],
        }
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let schema = user_schema();
        let row = vec![
            Value::Str("Alice".into()),
            Value::Str("alice@example.com".into()),
            Value::Int(30),
            Value::Bool(true),
        ];
        let encoded = encode_row(&schema, &row);
        let decoded = decode_row(&schema, &encoded);
        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded[0], Value::Str("Alice".into()));
        assert_eq!(decoded[1], Value::Str("alice@example.com".into()));
        assert_eq!(decoded[2], Value::Int(30));
        assert_eq!(decoded[3], Value::Bool(true));
    }

    #[test]
    fn test_encode_with_empty_optional() {
        let schema = user_schema();
        let row = vec![
            Value::Str("Bob".into()),
            Value::Str("bob@example.com".into()),
            Value::Empty,
            Value::Bool(false),
        ];
        let encoded = encode_row(&schema, &row);
        let decoded = decode_row(&schema, &encoded);
        assert_eq!(decoded[2], Value::Empty);
        assert_eq!(decoded[3], Value::Bool(false));
        assert_eq!(decoded[0], Value::Str("Bob".into()));
    }

    #[test]
    fn test_all_empty() {
        let schema = Schema {
            table_name: "t".into(),
            columns: vec![
                ColumnDef { name: "a".into(), type_id: TypeId::Int, required: false, position: 0 },
                ColumnDef { name: "b".into(), type_id: TypeId::Str, required: false, position: 1 },
            ],
        };
        let row = vec![Value::Empty, Value::Empty];
        let encoded = encode_row(&schema, &row);
        let decoded = decode_row(&schema, &encoded);
        assert_eq!(decoded[0], Value::Empty);
        assert_eq!(decoded[1], Value::Empty);
    }

    #[test]
    fn test_compact_overhead() {
        let schema = user_schema();
        let row = vec![
            Value::Str("Alice".into()),
            Value::Str("alice@example.com".into()),
            Value::Int(30),
            Value::Bool(true),
        ];
        let encoded = encode_row(&schema, &row);
        let pure_data = 5 + 17 + 8 + 1; // "Alice" + "alice@example.com" + i64 + bool = 31
        let overhead = encoded.len() - pure_data;
        // 2B length + 1B bitmap + 6B var offset table (3 entries * 2B) = 9B overhead
        assert!(overhead <= 10, "overhead was {overhead}, expected <= 10");
    }

    #[test]
    fn test_multiple_roundtrips() {
        let schema = Schema {
            table_name: "t".into(),
            columns: vec![
                ColumnDef { name: "id".into(), type_id: TypeId::Int, required: true, position: 0 },
                ColumnDef { name: "name".into(), type_id: TypeId::Str, required: true, position: 1 },
                ColumnDef { name: "score".into(), type_id: TypeId::Float, required: false, position: 2 },
                ColumnDef { name: "uuid".into(), type_id: TypeId::Uuid, required: false, position: 3 },
            ],
        };
        for i in 0..100 {
            let row = vec![
                Value::Int(i),
                Value::Str(format!("name_{i}")),
                if i % 3 == 0 { Value::Empty } else { Value::Float(i as f64 * 1.5) },
                if i % 5 == 0 { Value::Uuid([i as u8; 16]) } else { Value::Empty },
            ];
            let encoded = encode_row(&schema, &row);
            let decoded = decode_row(&schema, &encoded);
            assert_eq!(decoded, row, "roundtrip failed for i={i}");
        }
    }

    #[test]
    fn test_patch_var_column_same_size() {
        let schema = user_schema();
        let row = vec![
            Value::Str("Alice".into()),
            Value::Str("alice@example.com".into()),
            Value::Int(30),
            Value::Bool(true),
        ];
        let mut encoded = encode_row(&schema, &row);
        let layout = RowLayout::new(&schema);
        // name: "Alice" (5) → "Bobby" (5) — same size, trivial overwrite.
        let new_len = patch_var_column_in_place(&mut encoded, &layout, 0, Some(b"Bobby")).unwrap();
        encoded.truncate(new_len as usize);
        let decoded = decode_row(&schema, &encoded);
        assert_eq!(decoded[0], Value::Str("Bobby".into()));
        assert_eq!(decoded[1], Value::Str("alice@example.com".into()));
        assert_eq!(decoded[2], Value::Int(30));
        assert_eq!(decoded[3], Value::Bool(true));
    }

    #[test]
    fn test_patch_var_column_shrink_first() {
        let schema = user_schema();
        let row = vec![
            Value::Str("Alexandra".into()),              // 9 bytes
            Value::Str("alice@example.com".into()),
            Value::Int(42),
            Value::Bool(false),
        ];
        let mut encoded = encode_row(&schema, &row);
        let layout = RowLayout::new(&schema);
        // Patch `name` from 9 bytes → 3 bytes; trailing var data must shift back.
        let new_len = patch_var_column_in_place(&mut encoded, &layout, 0, Some(b"Eve")).unwrap();
        encoded.truncate(new_len as usize);
        let decoded = decode_row(&schema, &encoded);
        assert_eq!(decoded[0], Value::Str("Eve".into()));
        assert_eq!(decoded[1], Value::Str("alice@example.com".into()));
        assert_eq!(decoded[2], Value::Int(42));
        assert_eq!(decoded[3], Value::Bool(false));
    }

    #[test]
    fn test_patch_var_column_shrink_middle() {
        // Mirrors the Mission A bench: middle var col changes, trailing var
        // col must stay intact and its offset must slide back by `delta`.
        let schema = Schema {
            table_name: "U".into(),
            columns: vec![
                ColumnDef { name: "name".into(),   type_id: TypeId::Str, required: true,  position: 0 },
                ColumnDef { name: "status".into(), type_id: TypeId::Str, required: true,  position: 1 },
                ColumnDef { name: "email".into(),  type_id: TypeId::Str, required: true,  position: 2 },
                ColumnDef { name: "age".into(),    type_id: TypeId::Int, required: false, position: 3 },
            ],
        };
        let row = vec![
            Value::Str("user_42".into()),
            Value::Str("inactive".into()),              // 8 bytes
            Value::Str("user_42@example.com".into()),
            Value::Int(55),
        ];
        let mut encoded = encode_row(&schema, &row);
        let layout = RowLayout::new(&schema);
        let new_len = patch_var_column_in_place(&mut encoded, &layout, 1, Some(b"senior")).unwrap();
        encoded.truncate(new_len as usize);
        let decoded = decode_row(&schema, &encoded);
        assert_eq!(decoded[0], Value::Str("user_42".into()));
        assert_eq!(decoded[1], Value::Str("senior".into()));
        assert_eq!(decoded[2], Value::Str("user_42@example.com".into()));
        assert_eq!(decoded[3], Value::Int(55));
    }

    #[test]
    fn test_patch_var_column_grow_rejects() {
        let schema = user_schema();
        let row = vec![
            Value::Str("Al".into()),                    // 2 bytes
            Value::Str("alice@example.com".into()),
            Value::Int(30),
            Value::Bool(true),
        ];
        let mut encoded = encode_row(&schema, &row);
        let layout = RowLayout::new(&schema);
        assert!(patch_var_column_in_place(&mut encoded, &layout, 0, Some(b"Alexandra")).is_none());
    }

    #[test]
    fn test_patch_var_column_to_null() {
        let schema = user_schema();
        let row = vec![
            Value::Str("Alice".into()),
            Value::Str("alice@example.com".into()),
            Value::Int(30),
            Value::Bool(true),
        ];
        let mut encoded = encode_row(&schema, &row);
        let layout = RowLayout::new(&schema);
        // Set `name` to null.
        let new_len = patch_var_column_in_place(&mut encoded, &layout, 0, None).unwrap();
        encoded.truncate(new_len as usize);
        let decoded = decode_row(&schema, &encoded);
        assert_eq!(decoded[0], Value::Empty);
        assert_eq!(decoded[1], Value::Str("alice@example.com".into()));
    }

    #[test]
    fn test_patch_var_column_clears_null_bit() {
        let schema = Schema {
            table_name: "U".into(),
            columns: vec![
                ColumnDef { name: "label".into(), type_id: TypeId::Str, required: false, position: 0 },
                ColumnDef { name: "fill".into(),  type_id: TypeId::Str, required: false, position: 1 },
            ],
        };
        // Start with label = null; we need enough room in the (currently
        // 0-length) label slot to fit new content — which we don't have.
        // So this should reject.
        let row = vec![Value::Empty, Value::Str("data".into())];
        let mut encoded = encode_row(&schema, &row);
        let layout = RowLayout::new(&schema);
        // Attempting to write "x" into a currently 0-length var col should
        // be a grow → rejected.
        assert!(patch_var_column_in_place(&mut encoded, &layout, 0, Some(b"x")).is_none());
    }

    #[test]
    fn test_empty_string_vs_empty_set() {
        let schema = Schema {
            table_name: "t".into(),
            columns: vec![
                ColumnDef { name: "s".into(), type_id: TypeId::Str, required: false, position: 0 },
            ],
        };
        // Empty string is a real value, not Empty
        let row_str = vec![Value::Str("".into())];
        let row_empty = vec![Value::Empty];

        let enc_str = encode_row(&schema, &row_str);
        let enc_empty = encode_row(&schema, &row_empty);

        let dec_str = decode_row(&schema, &enc_str);
        let dec_empty = decode_row(&schema, &enc_empty);

        assert_eq!(dec_str[0], Value::Str("".into()));
        assert_eq!(dec_empty[0], Value::Empty);
        assert_ne!(dec_str[0], dec_empty[0]); // "" is NOT the same as {}
    }
}
