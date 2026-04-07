use crate::table::Table;
use crate::types::*;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// On-disk catalog file: lists every table's schema so we can reopen them
/// after a restart. Format is a small custom binary blob (no serde dep).
const CATALOG_FILE: &str = "catalog.bin";
const CATALOG_MAGIC: &[u8; 4] = b"BCAT";
const CATALOG_VERSION: u16 = 1;

/// System catalog: registry of all tables.
pub struct Catalog {
    tables: HashMap<String, Table>,
    data_dir: PathBuf,
}

impl Catalog {
    /// Create a brand-new catalog. Wipes any existing catalog file in this directory.
    pub fn create(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let cat = Catalog {
            tables: HashMap::new(),
            data_dir: data_dir.to_path_buf(),
        };
        cat.persist()?;
        Ok(cat)
    }

    /// Open an existing catalog from disk, rehydrating every table. If no
    /// catalog file is present this returns NotFound — callers can fall back
    /// to `create` for a fresh data dir.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        let cat_path = data_dir.join(CATALOG_FILE);
        if !cat_path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no catalog file"));
        }
        let schemas = read_catalog_file(&cat_path)?;
        let mut tables = HashMap::with_capacity(schemas.len());
        for schema in schemas {
            let name = schema.table_name.clone();
            let table = Table::open(schema, data_dir)?;
            tables.insert(name, table);
        }
        Ok(Catalog {
            tables,
            data_dir: data_dir.to_path_buf(),
        })
    }

    pub fn create_table(&mut self, schema: Schema) -> io::Result<()> {
        let name = schema.table_name.clone();
        if self.tables.contains_key(&name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, format!("table '{name}' already exists")));
        }
        let table = Table::create(schema, &self.data_dir)?;
        self.tables.insert(name, table);
        // Persist the updated catalog so the new schema survives a crash/restart.
        self.persist()?;
        Ok(())
    }

    /// Write the current set of schemas to disk atomically (write-then-rename).
    fn persist(&self) -> io::Result<()> {
        let cat_path = self.data_dir.join(CATALOG_FILE);
        let tmp_path = self.data_dir.join(format!("{CATALOG_FILE}.tmp"));
        let schemas: Vec<&Schema> = self.tables.values().map(|t| &t.schema).collect();
        write_catalog_file(&tmp_path, &schemas)?;
        fs::rename(&tmp_path, &cat_path)?;
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    pub fn insert(&mut self, table: &str, values: &Row) -> io::Result<RowId> {
        let t = self.tables.get_mut(table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("table '{table}' not found")))?;
        t.insert(values)
    }

    pub fn get(&self, table: &str, rid: RowId) -> Option<Row> {
        self.tables.get(table)?.get(rid)
    }

    pub fn delete(&mut self, table: &str, rid: RowId) -> io::Result<()> {
        let t = self.tables.get_mut(table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("table '{table}' not found")))?;
        t.delete(rid)
    }

    pub fn update(&mut self, table: &str, rid: RowId, values: &Row) -> io::Result<RowId> {
        let t = self.tables.get_mut(table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("table '{table}' not found")))?;
        t.update(rid, values)
    }

    pub fn scan(&self, table: &str) -> io::Result<impl Iterator<Item = (RowId, Row)> + '_> {
        let t = self.tables.get(table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("table '{table}' not found")))?;
        Ok(t.scan())
    }

    pub fn create_index(&mut self, table: &str, column: &str) -> io::Result<()> {
        let data_dir = self.data_dir.clone();
        let t = self.tables.get_mut(table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("table '{table}' not found")))?;
        t.create_index(column, &data_dir)
    }

    pub fn index_lookup(&self, table: &str, column: &str, key: &Value) -> io::Result<Option<Row>> {
        let t = self.tables.get(table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("table '{table}' not found")))?;
        Ok(t.index_lookup(column, key).map(|(_, row)| row))
    }

    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    pub fn schema(&self, table: &str) -> Option<&Schema> {
        self.tables.get(table).map(|t| &t.schema)
    }
}

// ─── Catalog file format ────────────────────────────────────────────────────
//
// Layout:
//   magic     [4]      = "BCAT"
//   version   u16
//   n_tables  u32
//   for each table:
//     table_name_len  u32
//     table_name      utf8 bytes
//     n_columns       u16
//     for each column:
//       name_len      u32
//       name          utf8 bytes
//       type_id       u8
//       required      u8
//       position      u16

fn write_catalog_file(path: &Path, schemas: &[&Schema]) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    buf.extend_from_slice(CATALOG_MAGIC);
    buf.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    buf.extend_from_slice(&(schemas.len() as u32).to_le_bytes());

    for schema in schemas {
        let name = schema.table_name.as_bytes();
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name);
        buf.extend_from_slice(&(schema.columns.len() as u16).to_le_bytes());
        for col in &schema.columns {
            let cn = col.name.as_bytes();
            buf.extend_from_slice(&(cn.len() as u32).to_le_bytes());
            buf.extend_from_slice(cn);
            buf.push(col.type_id as u8);
            buf.push(if col.required { 1 } else { 0 });
            buf.extend_from_slice(&col.position.to_le_bytes());
        }
    }

    let mut f = fs::OpenOptions::new()
        .create(true).write(true).truncate(true)
        .open(path)?;
    f.write_all(&buf)?;
    f.sync_data()?;
    Ok(())
}

fn read_catalog_file(path: &Path) -> io::Result<Vec<Schema>> {
    let mut f = fs::File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;

    let mut pos = 0usize;
    if buf.len() < 10 || &buf[0..4] != CATALOG_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad catalog magic"));
    }
    pos += 4;
    let version = u16::from_le_bytes(buf[pos..pos+2].try_into().unwrap());
    pos += 2;
    if version != CATALOG_VERSION {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("unsupported catalog version: {version}")));
    }
    let n_tables = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize;
    pos += 4;

    let mut schemas = Vec::with_capacity(n_tables);
    for _ in 0..n_tables {
        let name_len = read_u32(&buf, &mut pos)? as usize;
        let table_name = read_string(&buf, &mut pos, name_len)?;
        let n_cols = read_u16(&buf, &mut pos)? as usize;

        let mut columns = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            let cname_len = read_u32(&buf, &mut pos)? as usize;
            let name = read_string(&buf, &mut pos, cname_len)?;
            let type_id_raw = read_u8(&buf, &mut pos)?;
            let type_id = type_id_from_u8(type_id_raw)?;
            let required = read_u8(&buf, &mut pos)? != 0;
            let position = read_u16(&buf, &mut pos)?;
            columns.push(ColumnDef { name, type_id, required, position });
        }
        schemas.push(Schema { table_name, columns });
    }

    Ok(schemas)
}

fn read_u8(buf: &[u8], pos: &mut usize) -> io::Result<u8> {
    if *pos >= buf.len() { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated catalog")); }
    let v = buf[*pos];
    *pos += 1;
    Ok(v)
}
fn read_u16(buf: &[u8], pos: &mut usize) -> io::Result<u16> {
    if *pos + 2 > buf.len() { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated catalog")); }
    let v = u16::from_le_bytes(buf[*pos..*pos+2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}
fn read_u32(buf: &[u8], pos: &mut usize) -> io::Result<u32> {
    if *pos + 4 > buf.len() { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated catalog")); }
    let v = u32::from_le_bytes(buf[*pos..*pos+4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}
fn read_string(buf: &[u8], pos: &mut usize, len: usize) -> io::Result<String> {
    if *pos + len > buf.len() { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated catalog string")); }
    let s = std::str::from_utf8(&buf[*pos..*pos+len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 in catalog"))?
        .to_string();
    *pos += len;
    Ok(s)
}
fn type_id_from_u8(v: u8) -> io::Result<TypeId> {
    match v {
        0 => Ok(TypeId::Empty),
        1 => Ok(TypeId::Int),
        2 => Ok(TypeId::Float),
        3 => Ok(TypeId::Bool),
        4 => Ok(TypeId::Str),
        5 => Ok(TypeId::DateTime),
        6 => Ok(TypeId::Uuid),
        7 => Ok(TypeId::Bytes),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unknown type id: {v}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn temp_catalog(name: &str) -> Catalog {
        let dir = std::env::temp_dir().join(format!("powdb_cat_{name}_{}", std::process::id()));
        Catalog::create(&dir).unwrap()
    }

    #[test]
    fn test_create_table_and_insert() {
        let mut cat = temp_catalog("basic");
        let schema = Schema {
            table_name: "users".into(),
            columns: vec![
                ColumnDef { name: "name".into(), type_id: TypeId::Str, required: true, position: 0 },
                ColumnDef { name: "age".into(), type_id: TypeId::Int, required: false, position: 1 },
            ],
        };
        cat.create_table(schema).unwrap();

        let row = vec![Value::Str("Alice".into()), Value::Int(30)];
        let rid = cat.insert("users", &row).unwrap();

        let result = cat.get("users", rid).unwrap();
        assert_eq!(result[0], Value::Str("Alice".into()));
        assert_eq!(result[1], Value::Int(30));
    }

    #[test]
    fn test_scan_table() {
        let mut cat = temp_catalog("scan");
        let schema = Schema {
            table_name: "items".into(),
            columns: vec![
                ColumnDef { name: "name".into(), type_id: TypeId::Str, required: true, position: 0 },
                ColumnDef { name: "price".into(), type_id: TypeId::Float, required: true, position: 1 },
            ],
        };
        cat.create_table(schema).unwrap();

        for i in 0..50 {
            cat.insert("items", &vec![
                Value::Str(format!("item_{i}")),
                Value::Float(i as f64 * 1.5),
            ]).unwrap();
        }

        let rows: Vec<_> = cat.scan("items").unwrap().collect();
        assert_eq!(rows.len(), 50);
    }

    #[test]
    fn test_index_lookup() {
        let mut cat = temp_catalog("idx");
        let schema = Schema {
            table_name: "users".into(),
            columns: vec![
                ColumnDef { name: "email".into(), type_id: TypeId::Str, required: true, position: 0 },
                ColumnDef { name: "name".into(), type_id: TypeId::Str, required: true, position: 1 },
            ],
        };
        cat.create_table(schema).unwrap();
        cat.create_index("users", "email").unwrap();

        cat.insert("users", &vec![
            Value::Str("alice@example.com".into()),
            Value::Str("Alice".into()),
        ]).unwrap();
        cat.insert("users", &vec![
            Value::Str("bob@example.com".into()),
            Value::Str("Bob".into()),
        ]).unwrap();

        let result = cat.index_lookup("users", "email", &Value::Str("bob@example.com".into())).unwrap();
        assert!(result.is_some());
        let row = result.unwrap();
        assert_eq!(row[1], Value::Str("Bob".into()));
    }

    #[test]
    fn test_delete_row() {
        let mut cat = temp_catalog("delete");
        let schema = Schema {
            table_name: "t".into(),
            columns: vec![
                ColumnDef { name: "v".into(), type_id: TypeId::Int, required: true, position: 0 },
            ],
        };
        cat.create_table(schema).unwrap();
        let r1 = cat.insert("t", &vec![Value::Int(1)]).unwrap();
        let r2 = cat.insert("t", &vec![Value::Int(2)]).unwrap();
        cat.delete("t", r1).unwrap();
        assert!(cat.get("t", r1).is_none());
        assert!(cat.get("t", r2).is_some());
    }

    #[test]
    fn test_update_row() {
        let mut cat = temp_catalog("update");
        let schema = Schema {
            table_name: "t".into(),
            columns: vec![
                ColumnDef { name: "v".into(), type_id: TypeId::Int, required: true, position: 0 },
            ],
        };
        cat.create_table(schema).unwrap();
        let rid = cat.insert("t", &vec![Value::Int(1)]).unwrap();
        let new_rid = cat.update("t", rid, &vec![Value::Int(99)]).unwrap();
        let row = cat.get("t", new_rid).unwrap();
        assert_eq!(row[0], Value::Int(99));
    }

    #[test]
    fn test_persist_and_reopen() {
        let dir = std::env::temp_dir().join(format!("powdb_cat_persist_{}", std::process::id()));
        // Fresh dir
        let _ = std::fs::remove_dir_all(&dir);

        {
            let mut cat = Catalog::create(&dir).unwrap();
            cat.create_table(Schema {
                table_name: "users".into(),
                columns: vec![
                    ColumnDef { name: "name".into(), type_id: TypeId::Str, required: true, position: 0 },
                    ColumnDef { name: "age".into(), type_id: TypeId::Int, required: false, position: 1 },
                ],
            }).unwrap();
            cat.insert("users", &vec![Value::Str("Alice".into()), Value::Int(30)]).unwrap();
            cat.insert("users", &vec![Value::Str("Bob".into()), Value::Int(25)]).unwrap();
        }

        // Reopen — schema and rows should both still be there
        let cat = Catalog::open(&dir).unwrap();
        let schema = cat.schema("users").unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "name");
        assert_eq!(schema.columns[0].type_id, TypeId::Str);
        assert_eq!(schema.columns[1].type_id, TypeId::Int);

        let rows: Vec<_> = cat.scan("users").unwrap().collect();
        assert_eq!(rows.len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_open_missing_dir_errors() {
        let dir = std::env::temp_dir().join(format!("powdb_cat_missing_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // No catalog.bin yet
        assert!(Catalog::open(&dir).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list_tables() {
        let mut cat = temp_catalog("list");
        cat.create_table(Schema {
            table_name: "a".into(),
            columns: vec![ColumnDef { name: "x".into(), type_id: TypeId::Int, required: true, position: 0 }],
        }).unwrap();
        cat.create_table(Schema {
            table_name: "b".into(),
            columns: vec![ColumnDef { name: "y".into(), type_id: TypeId::Int, required: true, position: 0 }],
        }).unwrap();
        let mut tables = cat.list_tables();
        tables.sort();
        assert_eq!(tables, vec!["a", "b"]);
    }
}
