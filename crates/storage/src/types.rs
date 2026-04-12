use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// Type identifier for schema definitions and wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TypeId {
    Empty    = 0,
    Int      = 1,
    Float    = 2,
    Bool     = 3,
    Str      = 4,
    DateTime = 5,
    Uuid     = 6,
    Bytes    = 7,
}

/// A single scalar value. Optional fields use `Empty` (set-based nullability).
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    DateTime(i64),   // microseconds since Unix epoch
    Uuid([u8; 16]),
    Bytes(Vec<u8>),
    Empty,           // {} — the empty set, not NULL
}

impl Value {
    pub fn type_id(&self) -> TypeId {
        match self {
            Value::Int(_)      => TypeId::Int,
            Value::Float(_)    => TypeId::Float,
            Value::Bool(_)     => TypeId::Bool,
            Value::Str(_)      => TypeId::Str,
            Value::DateTime(_) => TypeId::DateTime,
            Value::Uuid(_)     => TypeId::Uuid,
            Value::Bytes(_)    => TypeId::Bytes,
            Value::Empty       => TypeId::Empty,
        }
    }

    /// Number of bytes this value occupies when encoded in a row.
    pub fn encoded_size(&self) -> usize {
        match self {
            Value::Int(_)      => 8,
            Value::Float(_)    => 8,
            Value::Bool(_)     => 1,
            Value::Str(s)      => 4 + s.len(),      // u32 length prefix + UTF-8 bytes
            Value::DateTime(_) => 8,
            Value::Uuid(_)     => 16,
            Value::Bytes(b)    => 4 + b.len(),       // u32 length prefix + raw bytes
            Value::Empty       => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Value::Empty)
    }
}

// NOTE on cross-numeric equality: `PartialEq` (and `Hash`) deliberately do
// NOT treat `Int(100)` and `Float(100.0)` as equal. Making them equal would
// require a consistent `Hash` — if `a == b` then `hash(a) == hash(b)` — and
// the canonical fix (normalise ints that fit exactly to f64 bits) is subtle
// enough that we intentionally keep equality/hashing strictly typed. The
// cross-type fix lives in `Ord::cmp` (below), which is what BETWEEN, ORDER
// BY, and range predicates actually call. If you need numeric equality
// across Int/Float, use `cmp(...) == Ordering::Equal` explicitly.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int(a), Value::Int(b))           => a == b,
            (Value::Float(a), Value::Float(b))       => a.total_cmp(b) == Ordering::Equal,
            (Value::Bool(a), Value::Bool(b))         => a == b,
            (Value::Str(a), Value::Str(b))           => a == b,
            (Value::DateTime(a), Value::DateTime(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b))         => a == b,
            (Value::Bytes(a), Value::Bytes(b))       => a == b,
            (Value::Empty, Value::Empty)             => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Tag first so distinct variants with coincidentally equal byte
        // representations (e.g. Int(0) vs Bool(false)) can't collide.
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Int(v)      => v.hash(state),
            // f64 has no Hash impl. Use the IEEE bit pattern, but canonicalise
            // via total_cmp so NaN hashes stably (and matches our PartialEq,
            // which also uses total_cmp for equality).
            Value::Float(v)    => v.to_bits().hash(state),
            Value::Bool(v)     => v.hash(state),
            Value::Str(v)      => v.hash(state),
            Value::DateTime(v) => v.hash(state),
            Value::Uuid(v)     => v.hash(state),
            Value::Bytes(v)    => v.hash(state),
            Value::Empty       => {} // discriminant already hashed
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b))           => a.cmp(b),
            (Value::Float(a), Value::Float(b))       => a.total_cmp(b),
            // Cross-type numeric comparison: promote Int -> f64 and use
            // total_cmp so BETWEEN / ORDER BY / range predicates work on
            // mixed Int literals vs Float columns (and vice versa).
            // `i64 as f64` can lose precision above 2^53, but the result is
            // still monotonic, which is what comparison needs.
            (Value::Int(a), Value::Float(b))         => (*a as f64).total_cmp(b),
            (Value::Float(a), Value::Int(b))         => a.total_cmp(&(*b as f64)),
            (Value::Bool(a), Value::Bool(b))         => a.cmp(b),
            (Value::Str(a), Value::Str(b))           => a.cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b))         => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b))       => a.cmp(b),
            (Value::Empty, Value::Empty)             => Ordering::Equal,
            (Value::Empty, _) => Ordering::Less,
            (_, Value::Empty) => Ordering::Greater,
            _ => (self.type_id() as u8).cmp(&(other.type_id() as u8)),
        }
    }
}

/// Column definition in a table schema.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub type_id: TypeId,
    pub required: bool,
    pub position: u16,
}

/// Schema for a table — ordered list of columns.
#[derive(Debug, Clone)]
pub struct Schema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Size of the null bitmap in bytes for this schema.
    pub fn null_bitmap_size(&self) -> usize {
        self.columns.len().div_ceil(8)
    }
}

/// Whether a type has a fixed encoded size.
pub fn is_fixed_size(type_id: TypeId) -> bool {
    matches!(type_id, TypeId::Int | TypeId::Float | TypeId::Bool | TypeId::DateTime | TypeId::Uuid)
}

/// Fixed encoded size for fixed-size types.
pub fn fixed_size(type_id: TypeId) -> Option<usize> {
    match type_id {
        TypeId::Int      => Some(8),
        TypeId::Float    => Some(8),
        TypeId::Bool     => Some(1),
        TypeId::DateTime => Some(8),
        TypeId::Uuid     => Some(16),
        _ => None,
    }
}

/// A row is an ordered list of values matching a schema.
pub type Row = Vec<Value>;

/// RowId uniquely identifies a row's physical location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowId {
    pub page_id: u32,
    pub slot_index: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_type_id() {
        assert_eq!(Value::Int(42).type_id(), TypeId::Int);
        assert_eq!(Value::Str("hello".into()).type_id(), TypeId::Str);
        assert_eq!(Value::Float(3.14).type_id(), TypeId::Float);
        assert_eq!(Value::Bool(true).type_id(), TypeId::Bool);
        assert_eq!(Value::Empty.type_id(), TypeId::Empty);
    }

    #[test]
    fn test_value_encoded_size() {
        assert_eq!(Value::Int(42).encoded_size(), 8);
        assert_eq!(Value::Float(1.0).encoded_size(), 8);
        assert_eq!(Value::Bool(true).encoded_size(), 1);
        assert_eq!(Value::Str("hello".into()).encoded_size(), 4 + 5);
        assert_eq!(Value::Empty.encoded_size(), 0);
    }

    #[test]
    fn test_value_ordering() {
        assert!(Value::Int(1) < Value::Int(2));
        assert!(Value::Str("a".into()) < Value::Str("b".into()));
        assert!(Value::Float(1.0) < Value::Float(2.0));
    }

    #[test]
    fn test_datetime_value() {
        let ts = Value::DateTime(1_700_000_000_000_000);
        assert_eq!(ts.type_id(), TypeId::DateTime);
        assert_eq!(ts.encoded_size(), 8);
    }

    #[test]
    fn test_uuid_value() {
        let uuid = Value::Uuid([0u8; 16]);
        assert_eq!(uuid.type_id(), TypeId::Uuid);
        assert_eq!(uuid.encoded_size(), 16);
    }

    #[test]
    fn test_empty_is_less_than_values() {
        assert!(Value::Empty < Value::Int(0));
        assert!(Value::Empty < Value::Str("".into()));
    }

    #[test]
    fn test_ord_int_vs_float() {
        // Regression: prior to the cross-type fix, `Int(100) < Float(175.5)`
        // fell through to comparing TypeId discriminants (Int=1 vs Float=2),
        // which happened to return Less for this case but Greater for others,
        // breaking BETWEEN on Float columns with Int literals.
        assert!(Value::Int(100) < Value::Float(175.5));
        assert!(Value::Int(500) > Value::Float(450.0));
        assert!(Value::Int(100) < Value::Float(100.5));
        assert!(Value::Int(100) > Value::Float(99.9));
        // Equal magnitudes compare equal across types.
        assert_eq!(Value::Int(100).cmp(&Value::Float(100.0)), Ordering::Equal);
        assert_eq!(Value::Int(0).cmp(&Value::Float(0.0)), Ordering::Equal);
        // Negative numbers.
        assert!(Value::Int(-10) < Value::Float(-5.5));
        assert!(Value::Int(-1) > Value::Float(-1.5));
    }

    #[test]
    fn test_ord_float_vs_int() {
        assert!(Value::Float(175.5) > Value::Int(100));
        assert!(Value::Float(450.0) < Value::Int(500));
        assert!(Value::Float(100.5) > Value::Int(100));
        assert!(Value::Float(99.9) < Value::Int(100));
        assert_eq!(Value::Float(100.0).cmp(&Value::Int(100)), Ordering::Equal);
        assert!(Value::Float(-5.5) > Value::Int(-10));
        assert!(Value::Float(-1.5) < Value::Int(-1));
    }

    #[test]
    fn test_ord_between_simulation() {
        // Simulates the Product.price BETWEEN 100 AND 500 case: Int literals
        // bounding a Float column. All of 175.5 and 450.0 must be in range.
        let lo = Value::Int(100);
        let hi = Value::Int(500);
        let prices = [29.0_f64, 175.5, 450.0, 1299.0];
        let in_range: Vec<f64> = prices
            .iter()
            .copied()
            .filter(|p| {
                let v = Value::Float(*p);
                v >= lo && v <= hi
            })
            .collect();
        assert_eq!(in_range, vec![175.5, 450.0]);
    }

    #[test]
    fn test_schema_column_lookup() {
        let schema = Schema {
            table_name: "test".into(),
            columns: vec![
                ColumnDef { name: "a".into(), type_id: TypeId::Int, required: true, position: 0 },
                ColumnDef { name: "b".into(), type_id: TypeId::Str, required: false, position: 1 },
            ],
        };
        assert_eq!(schema.column_index("a"), Some(0));
        assert_eq!(schema.column_index("b"), Some(1));
        assert_eq!(schema.column_index("c"), None);
        assert_eq!(schema.null_bitmap_size(), 1);
    }
}
