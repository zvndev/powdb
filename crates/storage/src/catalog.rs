use crate::table::Table;
use crate::types::*;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

/// System catalog: registry of all tables.
pub struct Catalog {
    tables: HashMap<String, Table>,
    data_dir: PathBuf,
}

impl Catalog {
    pub fn create(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        Ok(Catalog {
            tables: HashMap::new(),
            data_dir: data_dir.to_path_buf(),
        })
    }

    pub fn create_table(&mut self, schema: Schema) -> io::Result<()> {
        let name = schema.table_name.clone();
        let table = Table::create(schema, &self.data_dir)?;
        self.tables.insert(name, table);
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

#[cfg(test)]
mod tests {
    use super::*;
    fn temp_catalog(name: &str) -> Catalog {
        let dir = std::env::temp_dir().join(format!("batadb_cat_{name}_{}", std::process::id()));
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
