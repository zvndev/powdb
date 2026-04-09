#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Identifiers and literals
    Ident(String),       // User, name, email
    DotIdent(String),    // .name, .age (field access)
    IntLit(i64),         // 42
    FloatLit(f64),       // 3.14
    StringLit(String),   // "hello"
    BoolLit(bool),       // true, false
    Param(String),       // $age, $name (query parameter)

    // Keywords
    Type,       // type
    Filter,     // filter
    Order,      // order
    Limit,      // limit
    Offset,     // offset
    Insert,     // insert
    Update,     // update
    Delete,     // delete
    Upsert,     // upsert
    Select,     // select (alias for projection)
    Required,   // required
    Multi,      // multi
    Link,       // link
    Index,      // index
    On,         // on
    Asc,        // asc
    Desc,       // desc
    And,        // and
    Or,         // or
    Not,        // not
    Exists,     // exists
    Let,        // let
    As,         // as
    Match,      // match
    Group,      // group
    Join,       // join
    Inner,      // inner
    LeftKw,     // left  (keyword — avoids clashing with ast::JoinKind::LeftOuter naming)
    RightKw,    // right
    Outer,      // outer
    Cross,      // cross
    Transaction,// transaction
    View,       // view
    Materialized,// materialized
    Having,     // having
    Distinct,   // distinct
    In,         // in
    Between,    // between
    Like,       // like
    Count,      // count
    Avg,        // avg
    Sum,        // sum
    Min,        // min
    Max,        // max
    Is,         // is
    Null,       // null

    // String functions
    Upper,      // upper
    Lower,      // lower
    Length,     // length
    Trim,       // trim
    Substring,  // substring
    Concat,     // concat

    // CASE WHEN
    Case,       // case
    When,       // when
    Then,       // then
    Else,       // else
    End,        // end

    // DDL
    Alter,      // alter
    Drop,       // drop
    Add,        // add
    Column,     // column

    // Operators
    Eq,         // =
    Neq,        // !=
    Lt,         // <
    Gt,         // >
    Lte,        // <=
    Gte,        // >=
    Assign,     // :=
    Arrow,      // ->
    Pipe,       // |
    Coalesce,   // ??
    Plus,       // +
    Minus,      // -
    Star,       // *
    Slash,      // /

    // Delimiters
    LBrace,     // {
    RBrace,     // }
    LParen,     // (
    RParen,     // )
    Comma,      // ,
    Colon,      // :
    Dot,        // .

    // Special
    Eof,
}
