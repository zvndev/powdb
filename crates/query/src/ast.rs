/// Top-level PowQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Query(QueryExpr),
    Insert(InsertExpr),
    UpdateQuery(UpdateExpr),
    DeleteQuery(DeleteExpr),
    CreateType(CreateTypeExpr),
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
    Avg,
    Sum,
    Min,
    Max,
}

/// Expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Field(String),
    /// A table-qualified field reference: `table.field` or `alias.field`.
    /// Used by join queries to disambiguate columns that appear in multiple
    /// sources. The single-table read path never emits this variant, so
    /// existing fast paths keep matching `Expr::Field` unchanged.
    QualifiedField { qualifier: String, field: String },
    Literal(Literal),
    Param(String),
    BinaryOp(Box<Expr>, BinOp, Box<Expr>),
    UnaryOp(UnaryOp, Box<Expr>),
    FunctionCall(AggFunc, Box<Expr>),
    Coalesce(Box<Expr>, Box<Expr>),
    /// `expr in (val1, val2, ...)` or `expr not in (val1, val2, ...)`
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
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
    Eq, Neq, Lt, Gt, Lte, Gte,
    And, Or,
    Add, Sub, Mul, Div,
    Like,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryOp {
    Not,
    Exists,
    NotExists,
}
