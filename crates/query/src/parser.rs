use crate::ast::*;
use crate::lexer::lex;
use crate::token::Token;

#[derive(Debug)]
pub struct ParseError {
    pub message: String,
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

pub fn parse(input: &str) -> Result<Statement, ParseError> {
    let tokens = lex(input).map_err(|e| ParseError { message: e.message })?;
    let mut parser = Parser { tokens, pos: 0 };
    parser.parse_statement()
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        self.pos += 1;
        t
    }

    fn expect(&mut self, expected: &Token) -> Result<(), ParseError> {
        let t = self.advance();
        if &t == expected {
            Ok(())
        } else {
            Err(ParseError { message: format!("expected {expected:?}, got {t:?}") })
        }
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek() {
            Token::Insert => self.parse_insert(),
            Token::Type => self.parse_create_type(),
            Token::Count | Token::Avg | Token::Sum | Token::Min | Token::Max => {
                self.parse_aggregate_query()
            }
            Token::Ident(_) => self.parse_query_or_mutation(),
            _ => Err(ParseError { message: format!("unexpected token: {:?}", self.peek()) }),
        }
    }

    fn parse_query_or_mutation(&mut self) -> Result<Statement, ParseError> {
        let source = match self.advance() {
            Token::Ident(name) => name,
            t => return Err(ParseError { message: format!("expected type name, got {t:?}") }),
        };

        // Walk filter/order/limit/offset/projection, peeling off update/delete
        // mutations as we hit them. Anything else terminates the read pipeline
        // and we return a Query.
        let mut filter = None;
        let mut order = None;
        let mut limit = None;
        let mut offset = None;
        let mut projection = None;

        loop {
            match self.peek() {
                Token::Filter => {
                    self.advance();
                    filter = Some(self.parse_expr()?);
                }
                Token::Order => {
                    self.advance();
                    order = Some(self.parse_order()?);
                }
                Token::Limit => {
                    self.advance();
                    limit = Some(self.parse_expr()?);
                }
                Token::Offset => {
                    self.advance();
                    offset = Some(self.parse_expr()?);
                }
                Token::LBrace => {
                    projection = Some(self.parse_projection()?);
                }
                Token::Update => {
                    self.advance();
                    let assignments = self.parse_assignments()?;
                    return Ok(Statement::UpdateQuery(UpdateExpr { source, filter, assignments }));
                }
                Token::Delete => {
                    self.advance();
                    return Ok(Statement::DeleteQuery(DeleteExpr { source, filter }));
                }
                _ => break,
            }
        }

        Ok(Statement::Query(QueryExpr {
            source,
            filter,
            order,
            limit,
            offset,
            projection,
            aggregation: None,
        }))
    }

    /// Parse the read-only tail of a query (filter/order/limit/offset/projection)
    /// after `source` has already been consumed. Stops at the first token that
    /// isn't part of a read pipeline — the caller decides whether that's a
    /// terminator (RParen for an aggregate, EOF for a top-level query, etc.).
    /// Always returns `aggregation: None`; the caller layers that on.
    fn parse_query_tail(&mut self, source: String) -> Result<QueryExpr, ParseError> {
        let mut filter = None;
        let mut order = None;
        let mut limit = None;
        let mut offset = None;
        let mut projection = None;

        loop {
            match self.peek() {
                Token::Filter => {
                    self.advance();
                    filter = Some(self.parse_expr()?);
                }
                Token::Order => {
                    self.advance();
                    order = Some(self.parse_order()?);
                }
                Token::Limit => {
                    self.advance();
                    limit = Some(self.parse_expr()?);
                }
                Token::Offset => {
                    self.advance();
                    offset = Some(self.parse_expr()?);
                }
                Token::LBrace => {
                    projection = Some(self.parse_projection()?);
                }
                _ => break,
            }
        }

        Ok(QueryExpr {
            source,
            filter,
            order,
            limit,
            offset,
            projection,
            aggregation: None,
        })
    }

    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Insert)?;
        let target = match self.advance() {
            Token::Ident(name) => name,
            t => return Err(ParseError { message: format!("expected type name, got {t:?}") }),
        };
        let assignments = self.parse_assignments()?;
        Ok(Statement::Insert(InsertExpr { target, assignments }))
    }

    fn parse_assignments(&mut self) -> Result<Vec<Assignment>, ParseError> {
        self.expect(&Token::LBrace)?;
        let mut assignments = Vec::new();
        while *self.peek() != Token::RBrace {
            let field = match self.advance() {
                Token::Ident(name) => name,
                t => return Err(ParseError { message: format!("expected field name, got {t:?}") }),
            };
            self.expect(&Token::Assign)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { field, value });
            if *self.peek() == Token::Comma {
                self.advance();
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(assignments)
    }

    fn parse_projection(&mut self) -> Result<Vec<ProjectionField>, ParseError> {
        self.expect(&Token::LBrace)?;
        let mut fields = Vec::new();
        while *self.peek() != Token::RBrace {
            let first = self.advance();
            if *self.peek() == Token::Colon {
                // alias: expr
                self.advance();
                let alias = match first {
                    Token::Ident(name) => name,
                    _ => return Err(ParseError { message: "expected alias name".into() }),
                };
                let expr = self.parse_expr()?;
                fields.push(ProjectionField { alias: Some(alias), expr });
            } else {
                let expr = match first {
                    Token::Ident(name) => Expr::Field(name),
                    Token::DotIdent(name) => Expr::Field(name),
                    _ => return Err(ParseError { message: format!("expected field, got {first:?}") }),
                };
                fields.push(ProjectionField { alias: None, expr });
            }
            if *self.peek() == Token::Comma {
                self.advance();
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(fields)
    }

    fn parse_order(&mut self) -> Result<OrderClause, ParseError> {
        let field = match self.advance() {
            Token::DotIdent(name) => name,
            t => return Err(ParseError { message: format!("expected .field after order, got {t:?}") }),
        };
        let descending = match self.peek() {
            Token::Desc => { self.advance(); true }
            Token::Asc => { self.advance(); false }
            _ => false,
        };
        Ok(OrderClause { field, descending })
    }

    fn parse_aggregate_query(&mut self) -> Result<Statement, ParseError> {
        let func = match self.advance() {
            Token::Count => AggFunc::Count,
            Token::Avg => AggFunc::Avg,
            Token::Sum => AggFunc::Sum,
            Token::Min => AggFunc::Min,
            Token::Max => AggFunc::Max,
            t => return Err(ParseError { message: format!("expected aggregate function, got {t:?}") }),
        };
        self.expect(&Token::LParen)?;
        let source = match self.advance() {
            Token::Ident(name) => name,
            t => return Err(ParseError { message: format!("expected type name, got {t:?}") }),
        };
        // Allow a full read-pipeline tail inside the parens, e.g.
        // `count(User filter .age > 27 limit 100)`. parse_query_tail stops at
        // the first non-pipeline token, which here must be RParen.
        let mut query = self.parse_query_tail(source)?;
        self.expect(&Token::RParen)?;

        // For non-count aggregates, the caller typically writes the target
        // column via the trailing projection form:
        //     sum(User filter .age > 30 { .age })
        // We lift that single unaliased `.field` into AggregateExpr.field so
        // the executor's aggregate fast paths can see it. count() keeps its
        // projection if present (projection under count is silly but legal).
        let mut agg_field: Option<String> = None;
        if func != AggFunc::Count {
            if let Some(proj) = &query.projection {
                if proj.len() == 1 && proj[0].alias.is_none() {
                    if let Expr::Field(name) = &proj[0].expr {
                        agg_field = Some(name.clone());
                    }
                }
            }
            if agg_field.is_some() {
                query.projection = None;
            }
        }
        query.aggregation = Some(AggregateExpr { function: func, field: agg_field });
        Ok(Statement::Query(query))
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and_expr()?;
        while *self.peek() == Token::Or {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp(Box::new(left), BinOp::Or, Box::new(right));
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_comparison()?;
        while *self.peek() == Token::And {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp(Box::new(left), BinOp::And, Box::new(right));
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_additive()?;
        let op = match self.peek() {
            Token::Eq  => BinOp::Eq,
            Token::Neq => BinOp::Neq,
            Token::Lt  => BinOp::Lt,
            Token::Gt  => BinOp::Gt,
            Token::Lte => BinOp::Lte,
            Token::Gte => BinOp::Gte,
            _ => return Ok(left),
        };
        self.advance();
        let right = self.parse_additive()?;
        Ok(Expr::BinaryOp(Box::new(left), op, Box::new(right)))
    }

    fn parse_additive(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_primary()?;
        loop {
            let op = match self.peek() {
                Token::Plus  => BinOp::Add,
                Token::Minus => BinOp::Sub,
                Token::Coalesce => {
                    self.advance();
                    let right = self.parse_primary()?;
                    left = Expr::Coalesce(Box::new(left), Box::new(right));
                    continue;
                }
                _ => break,
            };
            self.advance();
            let right = self.parse_primary()?;
            left = Expr::BinaryOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.peek().clone() {
            Token::DotIdent(name) => {
                self.advance();
                Ok(Expr::Field(name))
            }
            Token::IntLit(v) => {
                self.advance();
                Ok(Expr::Literal(Literal::Int(v)))
            }
            Token::FloatLit(v) => {
                self.advance();
                Ok(Expr::Literal(Literal::Float(v)))
            }
            Token::StringLit(v) => {
                self.advance();
                Ok(Expr::Literal(Literal::String(v)))
            }
            Token::BoolLit(v) => {
                self.advance();
                Ok(Expr::Literal(Literal::Bool(v)))
            }
            Token::Param(name) => {
                self.advance();
                Ok(Expr::Param(name))
            }
            Token::Not => {
                self.advance();
                if *self.peek() == Token::Exists {
                    self.advance();
                    let expr = self.parse_primary()?;
                    Ok(Expr::UnaryOp(UnaryOp::NotExists, Box::new(expr)))
                } else {
                    let expr = self.parse_primary()?;
                    Ok(Expr::UnaryOp(UnaryOp::Not, Box::new(expr)))
                }
            }
            Token::Exists => {
                self.advance();
                let expr = self.parse_primary()?;
                Ok(Expr::UnaryOp(UnaryOp::Exists, Box::new(expr)))
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Token::Ident(name) => {
                self.advance();
                Ok(Expr::Field(name))
            }
            t => Err(ParseError { message: format!("unexpected token in expression: {t:?}") }),
        }
    }

    fn parse_create_type(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Type)?;
        let name = match self.advance() {
            Token::Ident(n) => n,
            t => return Err(ParseError { message: format!("expected type name, got {t:?}") }),
        };
        self.expect(&Token::LBrace)?;
        let mut fields = Vec::new();
        while *self.peek() != Token::RBrace {
            let required = if *self.peek() == Token::Required {
                self.advance();
                true
            } else {
                false
            };
            let field_name = match self.advance() {
                Token::Ident(n) => n,
                t => return Err(ParseError { message: format!("expected field name, got {t:?}") }),
            };
            self.expect(&Token::Colon)?;
            let type_name = match self.advance() {
                Token::Ident(n) => n,
                t => return Err(ParseError { message: format!("expected type name, got {t:?}") }),
            };
            fields.push(FieldDef { name: field_name, type_name, required });
            if *self.peek() == Token::Comma {
                self.advance();
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(Statement::CreateType(CreateTypeExpr { name, fields }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_simple_query() {
        let stmt = parse("User").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.source, "User");
                assert!(q.filter.is_none());
                assert!(q.projection.is_none());
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_filter() {
        let stmt = parse("User filter .age > 30").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.source, "User");
                assert!(q.filter.is_some());
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_projection() {
        let stmt = parse("User { name, email }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj.len(), 2);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_filter_order_limit() {
        let stmt = parse("User filter .age > 30 order .name desc limit 10").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert!(q.filter.is_some());
                let order = q.order.unwrap();
                assert_eq!(order.field, "name");
                assert!(order.descending);
                assert!(q.limit.is_some());
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse(r#"insert User { name := "Alice", age := 30 }"#).unwrap();
        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.target, "User");
                assert_eq!(ins.assignments.len(), 2);
                assert_eq!(ins.assignments[0].field, "name");
                assert_eq!(ins.assignments[1].field, "age");
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse(r#"User filter .email = "alice@ex.com" update { age := 31 }"#).unwrap();
        match stmt {
            Statement::UpdateQuery(upd) => {
                assert_eq!(upd.source, "User");
                assert!(upd.filter.is_some());
                assert_eq!(upd.assignments.len(), 1);
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse("User filter .age < 18 delete").unwrap();
        match stmt {
            Statement::DeleteQuery(del) => {
                assert_eq!(del.source, "User");
                assert!(del.filter.is_some());
            }
            _ => panic!("expected delete"),
        }
    }

    #[test]
    fn test_parse_count() {
        let stmt = parse("count(User)").unwrap();
        match stmt {
            Statement::Query(q) => {
                let agg = q.aggregation.unwrap();
                assert_eq!(agg.function, AggFunc::Count);
                assert!(q.filter.is_none());
            }
            _ => panic!("expected query with aggregation"),
        }
    }

    #[test]
    fn test_parse_count_with_filter() {
        // Regression: previously returned "expected RParen, got Filter".
        // count(<query>) must accept a full read-pipeline tail.
        let stmt = parse("count(User filter .age > 30)").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.source, "User");
                let agg = q.aggregation.unwrap();
                assert_eq!(agg.function, AggFunc::Count);
                assert!(q.filter.is_some(), "filter should have been parsed");
            }
            _ => panic!("expected query with aggregation"),
        }
    }

    #[test]
    fn test_parse_count_with_filter_and_limit() {
        let stmt = parse("count(User filter .age > 30 limit 100)").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.source, "User");
                assert!(q.filter.is_some());
                assert!(q.limit.is_some());
                assert_eq!(q.aggregation.unwrap().function, AggFunc::Count);
            }
            _ => panic!("expected query with aggregation"),
        }
    }

    #[test]
    fn test_parse_create_type() {
        let stmt = parse("type User { required name: str, age: int }").unwrap();
        match stmt {
            Statement::CreateType(ct) => {
                assert_eq!(ct.name, "User");
                assert_eq!(ct.fields.len(), 2);
                assert!(ct.fields[0].required);
                assert!(!ct.fields[1].required);
            }
            _ => panic!("expected create type"),
        }
    }

    #[test]
    fn test_parse_sum_with_field_projection() {
        // `sum(... { .age })` should lift `.age` into AggregateExpr.field and
        // clear the projection so the executor's aggregate fast path fires.
        let stmt = parse("sum(User filter .age > 30 { .age })").unwrap();
        match stmt {
            Statement::Query(q) => {
                let agg = q.aggregation.expect("aggregate");
                assert_eq!(agg.function, AggFunc::Sum);
                assert_eq!(agg.field.as_deref(), Some("age"));
                assert!(q.projection.is_none(), "projection should be lifted into agg.field");
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_avg_min_max_with_field() {
        for (src, expected) in [
            ("avg(User { .age })", AggFunc::Avg),
            ("min(User { .age })", AggFunc::Min),
            ("max(User { .age })", AggFunc::Max),
        ] {
            let stmt = parse(src).unwrap();
            match stmt {
                Statement::Query(q) => {
                    let agg = q.aggregation.unwrap();
                    assert_eq!(agg.function, expected, "func mismatch for {src}");
                    assert_eq!(agg.field.as_deref(), Some("age"), "field mismatch for {src}");
                    assert!(q.projection.is_none(), "projection should be cleared for {src}");
                }
                _ => panic!("expected query for {src}"),
            }
        }
    }

    #[test]
    fn test_parse_count_leaves_projection_alone() {
        // count() doesn't need a target field, so the projection (if any)
        // stays intact. It's silly to project inside a count, but it's legal.
        let stmt = parse("count(User { .age })").unwrap();
        match stmt {
            Statement::Query(q) => {
                let agg = q.aggregation.unwrap();
                assert_eq!(agg.function, AggFunc::Count);
                assert!(agg.field.is_none());
                assert!(q.projection.is_some(), "count must not eat projection");
            }
            _ => panic!("expected query"),
        }
    }
}
