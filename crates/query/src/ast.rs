/// Top-level PowQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Query(QueryExpr),
    Insert(InsertExpr),
    UpdateQuery(UpdateExpr),
    DeleteQuery(DeleteExpr),
    CreateType(CreateTypeExpr),
    AlterTable(AlterTableExpr),
    DropTable(DropTableExpr),
    CreateView(CreateViewExpr),
    RefreshView(RefreshViewExpr),
    DropView(DropViewExpr),
    Union(UnionExpr),
    Upsert(UpsertExpr),
    Explain(Box<Statement>),
}

/// `alter User add column status: str` / `alter User drop column status`
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableExpr {
    pub table: String,
    pub action: AlterAction,
}

/// An individual ALTER TABLE action.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    AddColumn {
        name: String,
        type_name: String,
        required: bool,
    },
    DropColumn {
        name: String,
    },
    /// `alter <Table> add index .<column>` — creates a B+Tree index on
    /// `column`. No-op if the index already exists.
    AddIndex {
        column: String,
    },
}

/// `drop User`
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableExpr {
    pub table: String,
}

/// `create [materialized] view ActiveUsers as User filter .active = true`
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewExpr {
    pub name: String,
    pub query: QueryExpr,
    /// The original source query text, stored for re-execution on refresh.
    pub query_text: String,
}

/// `refresh ActiveUsers`
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshViewExpr {
    pub name: String,
}

/// `drop view ActiveUsers`
#[derive(Debug, Clone, PartialEq)]
pub struct DropViewExpr {
    pub name: String,
}

/// `User filter .age > 30 union User filter .status = "vip"`
#[derive(Debug, Clone, PartialEq)]
pub struct UnionExpr {
    pub left: Box<Statement>,
    pub right: Box<Statement>,
    /// `true` for `union all` (keep duplicates), `false` for `union` (deduplicate).
    pub all: bool,
}

/// A query expression: Type [join ...]* [filter ...] [order ...] [limit ...] [{ projection }]
#[derive(Debug, Clone, PartialEq)]
pub struct QueryExpr {
    pub source: String,
    /// Optional alias for the primary source (e.g. `User as u`). Used to
    /// disambiguate qualified column references in join queries. `None` for
    /// single-table queries.
    pub alias: Option<String>,
    /// Zero or more join clauses chained to the primary source. For a
    /// single-table query this is always empty so existing code paths are
    /// untouched.
    pub joins: Vec<JoinClause>,
    pub filter: Option<Expr>,
    pub order: Option<OrderClause>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub projection: Option<Vec<ProjectionField>>,
    pub aggregation: Option<AggregateExpr>,
    pub distinct: bool,
    pub group_by: Option<GroupByClause>,
}

/// GROUP BY clause: `group .field1, .field2 [having <expr>]`.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupByClause {
    pub keys: Vec<String>,
    pub having: Option<Expr>,
}

/// A join clause appended to a query's primary source.
///
/// Example syntax (Mission E1.1 parser accepts this; executor still errors):
///   `User as u inner join Order as o on u.id = o.user_id filter o.total > 100`
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub source: String,
    pub alias: Option<String>,
    /// `on <expr>` — required for every kind except `Cross`.
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionField {
    pub alias: Option<String>,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderClause {
    pub keys: Vec<OrderKey>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderKey {
    pub field: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertExpr {
    pub target: String,
    pub assignments: Vec<Assignment>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateExpr {
    pub source: String,
    pub filter: Option<Expr>,
    pub assignments: Vec<Assignment>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteExpr {
    pub source: String,
    pub filter: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub field: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
/// `upsert User on .id { id := 1, name := "Alice" } [on conflict { name := "Alice" }]`
pub struct UpsertExpr {
    pub target: String,
    pub key_column: String,
    pub assignments: Vec<Assignment>,
    /// Assignments to apply on conflict. If empty, all non-key assignments
    /// from `assignments` are used as the update set.
    pub on_conflict: Vec<Assignment>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTypeExpr {
    pub name: String,
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub type_name: String,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub function: AggFunc,
    pub field: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggFunc {
    Count,
    CountDistinct,
    Avg,
    Sum,
    Min,
    Max,
}

/// Window function identifier.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Sum,
    Avg,
    Count,
    Min,
    Max,
}

/// Scalar (non-aggregate) function — operates on single values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScalarFn {
    Upper,
    Lower,
    Length,
    Trim,
    Substring, // substring(expr, start, len) — 1-indexed
    Concat,    // concat(expr, expr, ...) — variadic
    // Math
    Abs,
    Round, // round(expr) or round(expr, decimals)
    Ceil,
    Floor,
    Sqrt,
    Pow, // pow(base, exponent)
    // Date/time
    Now,      // now() — returns current unix timestamp in microseconds
    Extract,  // extract("year"|"month"|..., datetime_expr)
    DateAdd,  // date_add(datetime_expr, amount, "unit")
    DateDiff, // date_diff(dt1, dt2, "unit")
}

/// Target type for CAST expressions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CastType {
    Int,
    Float,
    Str,
    Bool,
    DateTime,
}

/// Expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Field(String),
    /// A table-qualified field reference: `table.field` or `alias.field`.
    /// Used by join queries to disambiguate columns that appear in multiple
    /// sources. The single-table read path never emits this variant, so
    /// existing fast paths keep matching `Expr::Field` unchanged.
    QualifiedField {
        qualifier: String,
        field: String,
    },
    Literal(Literal),
    Param(String),
    BinaryOp(Box<Expr>, BinOp, Box<Expr>),
    UnaryOp(UnaryOp, Box<Expr>),
    FunctionCall(AggFunc, Box<Expr>),
    /// Scalar (non-aggregate) function call.
    ScalarFunc(ScalarFn, Vec<Expr>),
    Coalesce(Box<Expr>, Box<Expr>),
    /// `expr in (val1, val2, ...)` or `expr not in (val1, val2, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `expr [not] in (subquery)` — the subquery is a full QueryExpr
    /// that produces a single column.
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<QueryExpr>,
        negated: bool,
    },
    /// `[not] exists (subquery)` — the subquery is a full QueryExpr.
    /// Currently uncorrelated only: the executor runs the subquery once
    /// before the scan loop and replaces this node with a Bool literal.
    ExistsSubquery {
        subquery: Box<QueryExpr>,
        negated: bool,
    },
    /// CASE WHEN ... THEN ... [ELSE ...] END
    Case {
        whens: Vec<(Box<Expr>, Box<Expr>)>,
        else_expr: Option<Box<Expr>>,
    },
    /// Window function: `func(args) over (partition ... order ...)`
    Window {
        function: WindowFunc,
        args: Vec<Expr>,
        partition_by: Vec<String>,
        order_by: Vec<OrderKey>,
    },
    /// Type cast: `cast(expr, "int")` or `cast(expr, "str")` etc.
    Cast(Box<Expr>, CastType),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Like,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryOp {
    Not,
    Exists,
    NotExists,
    IsNull,
    IsNotNull,
}
