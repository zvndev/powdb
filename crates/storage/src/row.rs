use crate::types::*;

/// Encode a row of values into the compact binary format.
///
/// Layout: [length: u16] [null_bitmap] [fixed columns packed] [var offset table] [var data]
///
/// Fixed columns are written in schema order, with placeholder zeros for Empty values.
/// Variable columns use an offset table (n_var + 1 entries) pointing into var data.
/// Overhead: 2 bytes (length) + ceil(n_cols/8) bytes (bitmap).
pub fn encode_row(schema: &Schema, values: &[Value]) -> Vec<u8> {
    debug_assert_eq!(values.len(), schema.columns.len());

    let n_cols = schema.columns.len();
    let bitmap_size = (n_cols + 7) / 8;

    // Build null bitmap: bit=1 means empty
    let mut null_bitmap = vec![0u8; bitmap_size];
    for (i, val) in values.iter().enumerate() {
        if val.is_empty() {
            null_bitmap[i / 8] |= 1 << (i % 8);
        }
    }

    // Encode fixed-size columns in schema order
    let mut fixed_buf = Vec::new();
    for (i, col) in schema.columns.iter().enumerate() {
        if !is_fixed_size(col.type_id) {
            continue;
        }
        if values[i].is_empty() {
            // Write zeros as placeholder so offsets stay predictable
            if let Some(sz) = fixed_size(col.type_id) {
                fixed_buf.extend_from_slice(&vec![0u8; sz]);
            }
        } else {
            match &values[i] {
                Value::Int(v)      => fixed_buf.extend_from_slice(&v.to_le_bytes()),
                Value::Float(v)    => fixed_buf.extend_from_slice(&v.to_le_bytes()),
                Value::Bool(v)     => fixed_buf.push(if *v { 1 } else { 0 }),
                Value::DateTime(v) => fixed_buf.extend_from_slice(&v.to_le_bytes()),
                Value::Uuid(v)     => fixed_buf.extend_from_slice(v),
                _ => unreachable!("fixed column with non-fixed value"),
            }
        }
    }

    // Collect variable-length columns
    let var_col_indices: Vec<usize> = schema.columns.iter().enumerate()
        .filter(|(_, c)| !is_fixed_size(c.type_id))
        .map(|(i, _)| i)
        .collect();

    let mut var_data = Vec::new();
    let mut var_offsets: Vec<u16> = Vec::with_capacity(var_col_indices.len() + 1);

    for &ci in &var_col_indices {
        var_offsets.push(var_data.len() as u16);
        match &values[ci] {
            Value::Str(s) => var_data.extend_from_slice(s.as_bytes()),
            Value::Bytes(b) => var_data.extend_from_slice(b),
            Value::Empty => {} // zero-length entry
            _ => unreachable!("variable column with non-variable value"),
        }
    }
    // End sentinel so we can compute lengths
    var_offsets.push(var_data.len() as u16);

    // Assemble
    let body_size = bitmap_size
        + fixed_buf.len()
        + var_offsets.len() * 2
        + var_data.len();
    let total_size = 2 + body_size; // 2B length prefix

    let mut buf = Vec::with_capacity(total_size);
    buf.extend_from_slice(&(total_size as u16).to_le_bytes());
    buf.extend_from_slice(&null_bitmap);
    buf.extend_from_slice(&fixed_buf);
    for off in &var_offsets {
        buf.extend_from_slice(&off.to_le_bytes());
    }
    buf.extend_from_slice(&var_data);

    debug_assert_eq!(buf.len(), total_size);
    buf
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
    pub fn fixed_offset(&self, col_idx: usize) -> Option<usize> {
        self.fixed_offsets[col_idx]
    }

    /// Size of the null bitmap in bytes.
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

/// Decode a row from its compact binary format back into Values.
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
