use rustc_hash::FxHashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const VIEW_FILE: &str = "views.bin";
const VIEW_MAGIC: &[u8; 4] = b"BVIW";
const VIEW_VERSION: u16 = 1;

/// Definition of a materialized view.
#[derive(Debug, Clone)]
pub struct ViewDef {
    /// View name (used as the backing table name too).
    pub name: String,
    /// Source PowQL query text. Re-executed on refresh.
    pub query: String,
    /// Tables this view depends on. Mutations to any of these mark the view
    /// dirty.
    pub depends_on: Vec<String>,
    /// Whether the cached result set is stale.
    pub dirty: bool,
}

/// Registry of all materialized views. Lives alongside the `Catalog` in the
/// `Engine` struct. Provides dirty-tracking and persistence.
pub struct ViewRegistry {
    views: FxHashMap<String, ViewDef>,
    /// Reverse index: base table name → list of view names that depend on it.
    /// Maintained in sync with `views` on every register/unregister.
    deps: FxHashMap<String, Vec<String>>,
    data_dir: PathBuf,
}

impl ViewRegistry {
    /// Create a new empty registry.
    pub fn new(data_dir: &Path) -> Self {
        ViewRegistry {
            views: FxHashMap::default(),
            deps: FxHashMap::default(),
            data_dir: data_dir.to_path_buf(),
        }
    }

    /// Open an existing registry from disk, or return an empty one if no
    /// views file exists.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        let path = data_dir.join(VIEW_FILE);
        if !path.exists() {
            return Ok(Self::new(data_dir));
        }
        let defs = read_view_file(&path)?;
        let mut reg = Self::new(data_dir);
        for def in defs {
            reg.insert_def(def);
        }
        Ok(reg)
    }

    /// Register a new view. Does NOT create the backing table or run the
    /// query — the executor handles that.
    pub fn register(&mut self, def: ViewDef) -> io::Result<()> {
        if self.views.contains_key(&def.name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("view '{}' already exists", def.name),
            ));
        }
        self.insert_def(def);
        self.persist()
    }

    /// Remove a view from the registry. Does NOT drop the backing table.
    pub fn unregister(&mut self, name: &str) -> io::Result<()> {
        let def = self.views.remove(name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("view '{name}' not found"))
        })?;
        for table in &def.depends_on {
            if let Some(list) = self.deps.get_mut(table) {
                list.retain(|v| v != name);
                if list.is_empty() {
                    self.deps.remove(table);
                }
            }
        }
        self.persist()
    }

    /// Look up a view by name.
    pub fn get(&self, name: &str) -> Option<&ViewDef> {
        self.views.get(name)
    }

    /// Check whether `name` is a registered materialized view.
    #[inline]
    pub fn is_view(&self, name: &str) -> bool {
        self.views.contains_key(name)
    }

    /// Mark a view as dirty (needs refresh before next read).
    pub fn mark_dirty(&mut self, view_name: &str) {
        if let Some(def) = self.views.get_mut(view_name) {
            def.dirty = true;
        }
    }

    /// Mark a view as clean after a successful refresh.
    pub fn mark_clean(&mut self, view_name: &str) {
        if let Some(def) = self.views.get_mut(view_name) {
            def.dirty = false;
        }
    }

    /// Check whether a view needs refresh.
    #[inline]
    pub fn is_dirty(&self, view_name: &str) -> bool {
        self.views.get(view_name).is_some_and(|d| d.dirty)
    }

    /// Mark all views that depend on `table` as dirty. Called by the
    /// executor after INSERT/UPDATE/DELETE on a base table.
    ///
    /// Returns immediately (no-op) when no views exist or no views depend
    /// on the given table — the hot path for tables with no dependents is
    /// a single `FxHashMap::get` returning `None`.
    #[inline]
    pub fn mark_dependents_dirty(&mut self, table: &str) {
        // Borrow the view names list first, then mutate views.
        // We need to collect to avoid double-borrow.
        let names: Option<Vec<String>> = self.deps.get(table)
            .cloned();
        if let Some(names) = names {
            for name in &names {
                if let Some(def) = self.views.get_mut(name.as_str()) {
                    def.dirty = true;
                }
            }
        }
    }

    /// List all view names.
    pub fn list_views(&self) -> Vec<&str> {
        self.views.keys().map(|k| k.as_str()).collect()
    }

    // ─── Internal ────────────────────────────────────────────────

    fn insert_def(&mut self, def: ViewDef) {
        let name = def.name.clone();
        for table in &def.depends_on {
            self.deps
                .entry(table.clone())
                .or_default()
                .push(name.clone());
        }
        self.views.insert(name, def);
    }

    fn persist(&self) -> io::Result<()> {
        let path = self.data_dir.join(VIEW_FILE);
        let tmp = self.data_dir.join(format!("{VIEW_FILE}.tmp"));
        let defs: Vec<&ViewDef> = self.views.values().collect();
        write_view_file(&tmp, &defs)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }
}

// ─── Binary format ──────────────────────────────────────────────────────
//
// Layout:
//   magic       [4]    = "BVIW"
//   version     u16    = 1
//   n_views     u32
//   for each view:
//     name_len    u32
//     name        utf8
//     query_len   u32
//     query       utf8
//     n_deps      u16
//     for each dep:
//       dep_len   u32
//       dep_name  utf8
//     dirty       u8

fn write_view_file(path: &Path, defs: &[&ViewDef]) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    buf.extend_from_slice(VIEW_MAGIC);
    buf.extend_from_slice(&VIEW_VERSION.to_le_bytes());
    buf.extend_from_slice(&(defs.len() as u32).to_le_bytes());

    for def in defs {
        let name = def.name.as_bytes();
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name);

        let query = def.query.as_bytes();
        buf.extend_from_slice(&(query.len() as u32).to_le_bytes());
        buf.extend_from_slice(query);

        buf.extend_from_slice(&(def.depends_on.len() as u16).to_le_bytes());
        for dep in &def.depends_on {
            let d = dep.as_bytes();
            buf.extend_from_slice(&(d.len() as u32).to_le_bytes());
            buf.extend_from_slice(d);
        }

        buf.push(if def.dirty { 1 } else { 0 });
    }

    let mut f = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    f.write_all(&buf)?;
    f.sync_data()?;
    Ok(())
}

fn read_view_file(path: &Path) -> io::Result<Vec<ViewDef>> {
    let mut f = fs::File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;

    let mut pos = 0usize;
    if buf.len() < 10 || &buf[0..4] != VIEW_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad view magic"));
    }
    pos += 4;
    let version = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap());
    pos += 2;
    if version != VIEW_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported view version: {version}"),
        ));
    }
    let n_views = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    let mut defs = Vec::with_capacity(n_views);
    for _ in 0..n_views {
        let name = read_str(&buf, &mut pos)?;
        let query = read_str(&buf, &mut pos)?;

        let n_deps = read_u16(&buf, &mut pos)? as usize;
        let mut depends_on = Vec::with_capacity(n_deps);
        for _ in 0..n_deps {
            depends_on.push(read_str(&buf, &mut pos)?);
        }

        let dirty = read_u8(&buf, &mut pos)? != 0;
        defs.push(ViewDef { name, query, depends_on, dirty });
    }
    Ok(defs)
}

fn read_u8(buf: &[u8], pos: &mut usize) -> io::Result<u8> {
    if *pos >= buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated view file"));
    }
    let v = buf[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u16(buf: &[u8], pos: &mut usize) -> io::Result<u16> {
    if *pos + 2 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated view file"));
    }
    let v = u16::from_le_bytes(buf[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

fn read_str(buf: &[u8], pos: &mut usize) -> io::Result<String> {
    if *pos + 4 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated view file"));
    }
    let len = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    if *pos + len > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated view file"));
    }
    let s = std::str::from_utf8(&buf[*pos..*pos + len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 in view file"))?
        .to_string();
    *pos += len;
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_registry(name: &str) -> ViewRegistry {
        let dir = std::env::temp_dir().join(format!("powdb_view_{name}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        ViewRegistry::new(&dir)
    }

    #[test]
    fn test_register_and_lookup() {
        let mut reg = temp_registry("basic");
        reg.register(ViewDef {
            name: "ActiveUsers".into(),
            query: "User filter .active = true".into(),
            depends_on: vec!["User".into()],
            dirty: false,
        }).unwrap();
        assert!(reg.is_view("ActiveUsers"));
        assert!(!reg.is_view("User"));
        let def = reg.get("ActiveUsers").unwrap();
        assert_eq!(def.query, "User filter .active = true");
    }

    #[test]
    fn test_dirty_tracking() {
        let mut reg = temp_registry("dirty");
        reg.register(ViewDef {
            name: "V1".into(),
            query: "T1".into(),
            depends_on: vec!["T1".into()],
            dirty: false,
        }).unwrap();
        assert!(!reg.is_dirty("V1"));
        reg.mark_dependents_dirty("T1");
        assert!(reg.is_dirty("V1"));
        reg.mark_clean("V1");
        assert!(!reg.is_dirty("V1"));
    }

    #[test]
    fn test_multi_dependency() {
        let mut reg = temp_registry("multi");
        reg.register(ViewDef {
            name: "V1".into(),
            query: "T1 inner join T2 on .id = .fk".into(),
            depends_on: vec!["T1".into(), "T2".into()],
            dirty: false,
        }).unwrap();
        // Mutating either dependency dirties the view
        reg.mark_dependents_dirty("T2");
        assert!(reg.is_dirty("V1"));
    }

    #[test]
    fn test_unregister() {
        let mut reg = temp_registry("unreg");
        reg.register(ViewDef {
            name: "V1".into(),
            query: "T1".into(),
            depends_on: vec!["T1".into()],
            dirty: false,
        }).unwrap();
        reg.unregister("V1").unwrap();
        assert!(!reg.is_view("V1"));
        // Dependency map is cleaned up — marking T1 dirty doesn't panic
        reg.mark_dependents_dirty("T1");
    }

    #[test]
    fn test_persist_and_reopen() {
        let dir = std::env::temp_dir().join(format!("powdb_view_persist_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        {
            let mut reg = ViewRegistry::new(&dir);
            reg.register(ViewDef {
                name: "V1".into(),
                query: "User filter .active = true".into(),
                depends_on: vec!["User".into()],
                dirty: true,
            }).unwrap();
        }
        // Reopen
        let reg = ViewRegistry::open(&dir).unwrap();
        assert!(reg.is_view("V1"));
        let def = reg.get("V1").unwrap();
        assert_eq!(def.query, "User filter .active = true");
        assert!(def.dirty);
        assert_eq!(def.depends_on, vec!["User".to_string()]);
    }
}
