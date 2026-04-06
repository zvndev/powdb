use std::cmp::Ordering;

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

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int(a), Value::Int(b))           => a == b,
            (Value::Float(a), Value::Float(b))       => a.to_bits() == b.to_bits(),
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
        (self.columns.len() + 7) / 8
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
