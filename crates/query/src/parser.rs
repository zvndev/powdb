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
        let stmt = match self.peek() {
            Token::Insert => self.parse_insert(),
            Token::Type => self.parse_create_type(),
            Token::Alter => self.parse_alter_table(),
            Token::Drop => self.parse_drop_or_drop_view(),
            Token::Materialized => self.parse_create_view(),
            Token::Refresh => self.parse_refresh_view(),
            Token::Count | Token::Avg | Token::Sum | Token::Min | Token::Max => {
                self.parse_aggregate_query()
            }
            Token::Ident(_) => self.parse_query_or_mutation(),
            _ => Err(ParseError { message: format!("unexpected token: {:?}", self.peek()) }),
        }?;
        // Check for UNION chaining after any query-producing statement.
        self.maybe_parse_union(stmt)
    }

    fn parse_query_or_mutation(&mut self) -> Result<Statement, ParseError> {
        let source = match self.advance() {
            Token::Ident(name) => name,
            t => return Err(ParseError { message: format!("expected type name, got {t:?}") }),
        };
        let alias = self.try_parse_alias();
        let joins = self.parse_joins()?;

        // Walk filter/order/limit/offset/projection, peeling off update/delete
        // mutations as we hit them. Anything else terminates the read pipeline
        // and we return a Query.
        let mut filter = None;
        let mut order = None;
        let mut limit = None;
        let mut offset = None;
        let mut projection = None;
        let mut distinct = false;
        let mut group_by = None;

        loop {
            match self.peek() {
                Token::Distinct => {
                    self.advance();
                    distinct = true;
                }
                Token::Group => {
                    self.advance();
                    group_by = Some(self.parse_group_by()?);
                }
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
                    if !joins.is_empty() {
                        return Err(ParseError {
                            message: "update on a joined query is not supported".into(),
                        });
                    }
                    self.advance();
                    let assignments = self.parse_assignments()?;
                    return Ok(Statement::UpdateQuery(UpdateExpr { source, filter, assignments }));
                }
                Token::Delete => {
                    if !joins.is_empty() {
                        return Err(ParseError {
                            message: "delete on a joined query is not supported".into(),
                        });
                    }
                    self.advance();
                    return Ok(Statement::DeleteQuery(DeleteExpr { source, filter }));
                }
                _ => break,
            }
        }

        Ok(Statement::Query(QueryExpr {
            source,
            alias,
            joins,
            filter,
            order,
            limit,
            offset,
            projection,
            aggregation: None,
            distinct,
            group_by,
        }))
    }

    /// Parse the read-only tail of a query (filter/order/limit/offset/projection)
    /// after `source` has already been consumed. Stops at the first token that
    /// isn't part of a read pipeline — the caller decides whether that's a
    /// terminator (RParen for an aggregate, EOF for a top-level query, etc.).
    /// Always returns `aggregation: None`; the caller layers that on.
    fn parse_query_tail(&mut self, source: String) -> Result<QueryExpr, ParseError> {
        let alias = self.try_parse_alias();
        let joins = self.parse_joins()?;
        let mut filter = None;
        let mut order = None;
        let mut limit = None;
        let mut offset = None;
        let mut projection = None;
        let mut distinct = false;
        let mut group_by = None;

        loop {
            match self.peek() {
                Token::Distinct => {
                    self.advance();
                    distinct = true;
                }
                Token::Group => {
                    self.advance();
                    group_by = Some(self.parse_group_by()?);
                }
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
            alias,
            joins,
            filter,
            order,
            limit,
            offset,
            projection,
            aggregation: None,
            distinct,
            group_by,
        })
    }

    /// Consume an optional `as <ident>` suffix on a source. Returns `None`
    /// if the next token isn't `as`. Used by both the primary source and each
    /// join source so queries can disambiguate columns via `alias.field`.
    fn try_parse_alias(&mut self) -> Option<String> {
        if *self.peek() == Token::As {
            self.advance();
            if let Token::Ident(name) = self.peek().clone() {
                self.advance();
                return Some(name);
            }
        }
        None
    }

    /// Parse zero or more join clauses. Each clause is:
    ///   (`inner` | `left` [`outer`] | `right` [`outer`] | `cross`)? `join`
    ///   <Ident> [`as` <ident>] [`on` <expr>]
    ///
    /// `on` is required for every kind except `cross`. The default kind is
    /// `inner` when the caller wrote bare `join` without a preceding modifier.
    fn parse_joins(&mut self) -> Result<Vec<JoinClause>, ParseError> {
        let mut joins = Vec::new();
        loop {
            let kind = match self.peek() {
                Token::Join => {
                    self.advance();
                    JoinKind::Inner
                }
                Token::Inner => {
                    self.advance();
                    self.expect(&Token::Join)?;
                    JoinKind::Inner
                }
                Token::LeftKw => {
                    self.advance();
                    if *self.peek() == Token::Outer {
                        self.advance();
                    }
                    self.expect(&Token::Join)?;
                    JoinKind::LeftOuter
                }
                Token::RightKw => {
                    self.advance();
                    if *self.peek() == Token::Outer {
                        self.advance();
                    }
                    self.expect(&Token::Join)?;
                    JoinKind::RightOuter
                }
                Token::Cross => {
                    self.advance();
                    self.expect(&Token::Join)?;
                    JoinKind::Cross
                }
                _ => break,
            };

            let source = match self.advance() {
                Token::Ident(name) => name,
                t => {
                    return Err(ParseError {
                        message: format!("expected type name after join, got {t:?}"),
                    });
                }
            };
            let alias = self.try_parse_alias();
            let on = if kind == JoinKind::Cross {
                None
            } else if *self.peek() == Token::On {
                self.advance();
                Some(self.parse_expr()?)
            } else {
                return Err(ParseError {
                    message: format!("expected `on <expr>` after join {source}"),
                });
            };

            joins.push(JoinClause { kind, source, alias, on });
        }
        Ok(joins)
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
                    // Mission E1.2: `{ u.name }` — a qualifier followed by
                    // `.field` folds into a QualifiedField so join projections
                    // can pull from a specific source.
                    Token::Ident(name) => {
                        if let Token::DotIdent(field) = self.peek().clone() {
                            self.advance();
                            Expr::QualifiedField { qualifier: name, field }
                        } else {
                            Expr::Field(name)
                        }
                    }
                    Token::DotIdent(name) => Expr::Field(name),
                    Token::RowNumber | Token::Rank | Token::DenseRank => {
                        let wfunc = match first {
                            Token::RowNumber  => WindowFunc::RowNumber,
                            Token::Rank       => WindowFunc::Rank,
                            Token::DenseRank  => WindowFunc::DenseRank,
                            _ => return Err(ParseError { message: "unexpected window function token".into() }),
                        };
                        self.expect(&Token::LParen)?;
                        self.expect(&Token::RParen)?;
                        let (partition_by, order_by) = self.parse_over_clause()?;
                        Expr::Window { function: wfunc, args: vec![], partition_by, order_by }
                    }
                    Token::Count | Token::Avg | Token::Sum | Token::Min | Token::Max => {
                        let mut func = match first {
                            Token::Count => AggFunc::Count,
                            Token::Avg   => AggFunc::Avg,
                            Token::Sum   => AggFunc::Sum,
                            Token::Min   => AggFunc::Min,
                            Token::Max   => AggFunc::Max,
                            _ => return Err(ParseError { message: "unexpected aggregate token".into() }),
                        };
                        self.expect(&Token::LParen)?;
                        // count(*) — count all rows
                        if func == AggFunc::Count && *self.peek() == Token::Star {
                            self.advance();
                            self.expect(&Token::RParen)?;
                            // Check for OVER — count(*) over (...)
                            if *self.peek() == Token::Over {
                                let (partition_by, order_by) = self.parse_over_clause()?;
                                Expr::Window {
                                    function: WindowFunc::Count,
                                    args: vec![Expr::Field("*".into())],
                                    partition_by,
                                    order_by,
                                }
                            } else {
                                Expr::FunctionCall(AggFunc::Count, Box::new(Expr::Field("*".into())))
                            }
                        } else {
                        // count(distinct .field) → CountDistinct
                        if func == AggFunc::Count && *self.peek() == Token::Distinct {
                            self.advance();
                            func = AggFunc::CountDistinct;
                        }
                        let inner = self.parse_expr()?;
                        self.expect(&Token::RParen)?;
                        // Check for OVER — e.g. sum(.salary) over (...)
                        if *self.peek() == Token::Over {
                            let wfunc = match func {
                                AggFunc::Count => WindowFunc::Count,
                                AggFunc::Avg   => WindowFunc::Avg,
                                AggFunc::Sum   => WindowFunc::Sum,
                                AggFunc::Min   => WindowFunc::Min,
                                AggFunc::Max   => WindowFunc::Max,
                                _ => return Err(ParseError {
                                    message: "count(distinct ...) over (...) is not supported".into(),
                                }),
                            };
                            let (partition_by, order_by) = self.parse_over_clause()?;
                            Expr::Window { function: wfunc, args: vec![inner], partition_by, order_by }
                        } else {
                            Expr::FunctionCall(func, Box::new(inner))
                        }
                        }
                    }
                    Token::Upper | Token::Lower | Token::Length | Token::Trim
                    | Token::Substring | Token::Concat => {
                        let func = match first {
                            Token::Upper => ScalarFn::Upper,
                            Token::Lower => ScalarFn::Lower,
                            Token::Length => ScalarFn::Length,
                            Token::Trim => ScalarFn::Trim,
                            Token::Substring => ScalarFn::Substring,
                            Token::Concat => ScalarFn::Concat,
                            _ => unreachable!(),
                        };
                        self.expect(&Token::LParen)?;
                        let mut args = Vec::new();
                        while *self.peek() != Token::RParen {
                            args.push(self.parse_expr()?);
                            if *self.peek() == Token::Comma { self.advance(); }
                        }
                        self.expect(&Token::RParen)?;
                        Expr::ScalarFunc(func, args)
                    }
                    Token::Case => {
                        let mut whens = Vec::new();
                        while *self.peek() == Token::When {
                            self.advance();
                            let condition = self.parse_expr()?;
                            self.expect(&Token::Then)?;
                            let result = self.parse_expr()?;
                            whens.push((Box::new(condition), Box::new(result)));
                        }
                        let else_expr = if *self.peek() == Token::Else {
                            self.advance();
                            Some(Box::new(self.parse_expr()?))
                        } else {
                            None
                        };
                        self.expect(&Token::End)?;
                        Expr::Case { whens, else_expr }
                    }
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

    /// Parse the OVER clause for a window function:
    /// `over (partition .col1, .col2 order .col3 asc, .col4 desc)`
    fn parse_over_clause(&mut self) -> Result<(Vec<String>, Vec<OrderKey>), ParseError> {
        self.expect(&Token::Over)?;
        self.expect(&Token::LParen)?;
        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        if *self.peek() == Token::Partition {
            self.advance();
            loop {
                match self.peek() {
                    Token::DotIdent(name) => {
                        let name = name.clone();
                        self.advance();
                        partition_by.push(name);
                    }
                    _ => break,
                }
                if *self.peek() == Token::Comma {
                    // Only consume comma if the next token is another DotIdent
                    // (i.e. still in the partition list). If the next meaningful
                    // token is `order`, `RParen`, etc., stop.
                    if matches!(self.tokens.get(self.pos + 1), Some(Token::DotIdent(_))) {
                        self.advance();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        if *self.peek() == Token::Order {
            self.advance();
            loop {
                let field = match self.peek() {
                    Token::DotIdent(name) => {
                        let name = name.clone();
                        self.advance();
                        name
                    }
                    _ => break,
                };
                let descending = match self.peek() {
                    Token::Desc => { self.advance(); true }
                    Token::Asc => { self.advance(); false }
                    _ => false,
                };
                order_by.push(OrderKey { field, descending });
                if *self.peek() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
        }
        self.expect(&Token::RParen)?;
        Ok((partition_by, order_by))
    }

    fn parse_order(&mut self) -> Result<OrderClause, ParseError> {
        let mut keys = Vec::new();
        loop {
            let field = match self.advance() {
                Token::DotIdent(name) => name,
                t => return Err(ParseError { message: format!("expected .field after order, got {t:?}") }),
            };
            let descending = match self.peek() {
                Token::Desc => { self.advance(); true }
                Token::Asc => { self.advance(); false }
                _ => false,
            };
            keys.push(OrderKey { field, descending });
            if *self.peek() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        Ok(OrderClause { keys })
    }

    fn parse_aggregate_query(&mut self) -> Result<Statement, ParseError> {
        let mut func = match self.advance() {
            Token::Count => AggFunc::Count,
            Token::Avg => AggFunc::Avg,
            Token::Sum => AggFunc::Sum,
            Token::Min => AggFunc::Min,
            Token::Max => AggFunc::Max,
            t => return Err(ParseError { message: format!("expected aggregate function, got {t:?}") }),
        };
        self.expect(&Token::LParen)?;
        // count(distinct User ...) → CountDistinct
        if func == AggFunc::Count && *self.peek() == Token::Distinct {
            self.advance();
            func = AggFunc::CountDistinct;
        }
        let source = match self.advance() {
            Token::Ident(name) => name,
            t => return Err(ParseError { message: format!("expected type name, got {t:?}") }),
        };
        // Allow a full read-pipeline tail inside the parens, e.g.
        // `count(User filter .age > 27 limit 100)`. parse_query_tail stops at
        // the first non-pipeline token, which here must be RParen.
        let mut query = self.parse_query_tail(source)?;
        self.expect(&Token::RParen)?;

        // For non-count aggregates (and count distinct), the caller typically
        // writes the target column via the trailing projection form:
        //     sum(User filter .age > 30 { .age })
        //     count(distinct User { .name })
        // We lift that single unaliased `.field` into AggregateExpr.field so
        // the executor's aggregate fast paths can see it.
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

        // IS NULL / IS NOT NULL (postfix)
        if *self.peek() == Token::Is {
            self.advance();
            if *self.peek() == Token::Not {
                self.advance();
                self.expect(&Token::Null)?;
                return Ok(Expr::UnaryOp(UnaryOp::IsNotNull, Box::new(left)));
            } else {
                self.expect(&Token::Null)?;
                return Ok(Expr::UnaryOp(UnaryOp::IsNull, Box::new(left)));
            }
        }

        // Postfix: `in (...)`, `like "..."`, `between X and Y`
        // and their negated forms: `not in`, `not like`, `not between`.
        match self.peek() {
            Token::In => {
                self.advance();
                return self.parse_in_list(left, false);
            }
            Token::Like => {
                self.advance();
                let pattern = self.parse_additive()?;
                return Ok(Expr::BinaryOp(Box::new(left), BinOp::Like, Box::new(pattern)));
            }
            Token::Between => {
                self.advance();
                return self.parse_between(left, false);
            }
            Token::Not => {
                // Peek ahead: `not in`, `not like`, `not between`.
                // If the token after `not` isn't one of these, don't consume
                // `not` — let the caller handle it.
                let next = self.tokens.get(self.pos + 1);
                match next {
                    Some(Token::In) => {
                        self.advance(); // not
                        self.advance(); // in
                        return self.parse_in_list(left, true);
                    }
                    Some(Token::Like) => {
                        self.advance(); // not
                        self.advance(); // like
                        let pattern = self.parse_additive()?;
                        let like = Expr::BinaryOp(Box::new(left), BinOp::Like, Box::new(pattern));
                        return Ok(Expr::UnaryOp(UnaryOp::Not, Box::new(like)));
                    }
                    Some(Token::Between) => {
                        self.advance(); // not
                        self.advance(); // between
                        return self.parse_between(left, true);
                    }
                    _ => {}
                }
            }
            _ => {}
        }

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

    /// Parse `(val1, val2, ...)` or `(subquery)` after `in` / `not in`.
    /// A subquery is detected by `(` followed by an `Ident` that is NOT
    /// followed by `,` or `)` — in PowQL, bare identifiers in value lists
    /// don't appear (field refs start with `.`).
    fn parse_in_list(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParseError> {
        self.expect(&Token::LParen)?;
        // Detect subquery: `( Ident ...` where the Ident is a table name.
        if let Token::Ident(_) = self.peek() {
            // Peek further: if the next token after the Ident is NOT `,` or
            // `)`, it's a subquery source name.
            let after = self.tokens.get(self.pos + 1);
            let is_subquery = !matches!(after, Some(Token::Comma) | Some(Token::RParen));
            if is_subquery {
                let source = match self.advance() {
                    Token::Ident(name) => name,
                    _ => unreachable!(),
                };
                let subquery = self.parse_query_tail(source)?;
                self.expect(&Token::RParen)?;
                return Ok(Expr::InSubquery {
                    expr: Box::new(expr),
                    subquery: Box::new(subquery),
                    negated,
                });
            }
        }
        let mut list = Vec::new();
        while *self.peek() != Token::RParen {
            list.push(self.parse_expr()?);
            if *self.peek() == Token::Comma {
                self.advance();
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Expr::InList { expr: Box::new(expr), list, negated })
    }

    /// Try to parse a `(subquery)` tail for `exists` / `not exists`.
    /// A subquery is detected when the next tokens are `( Ident ...` —
    /// bare identifiers inside parens are always table/view names in
    /// PowQL (column refs start with `.`). Returns `Ok(Some(query))` if
    /// consumed, `Ok(None)` if the shape doesn't match (so the caller
    /// falls back to parsing a scalar primary for the legacy
    /// `exists <expr>` form).
    fn try_parse_exists_subquery(&mut self) -> Result<Option<QueryExpr>, ParseError> {
        if *self.peek() != Token::LParen {
            return Ok(None);
        }
        // Peek one token inside the paren. Anything starting with `Ident`
        // is a source name — PowQL column references use `DotIdent`, so
        // an `exists (X ...)` with a bare `X` is unambiguously a subquery.
        let after_lparen = self.tokens.get(self.pos + 1);
        if !matches!(after_lparen, Some(Token::Ident(_))) {
            return Ok(None);
        }
        self.expect(&Token::LParen)?;
        let source = match self.advance() {
            Token::Ident(name) => name,
            _ => unreachable!(),
        };
        let subquery = self.parse_query_tail(source)?;
        self.expect(&Token::RParen)?;
        Ok(Some(subquery))
    }

    /// Parse `low and high` after `between` / `not between`.
    /// Desugars into `expr >= low AND expr <= high` (or negated:
    /// `expr < low OR expr > high`).
    fn parse_between(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParseError> {
        let low = self.parse_additive()?;
        self.expect(&Token::And)?;
        let high = self.parse_additive()?;
        if negated {
            // NOT BETWEEN: expr < low OR expr > high
            Ok(Expr::BinaryOp(
                Box::new(Expr::BinaryOp(Box::new(expr.clone()), BinOp::Lt, Box::new(low))),
                BinOp::Or,
                Box::new(Expr::BinaryOp(Box::new(expr), BinOp::Gt, Box::new(high))),
            ))
        } else {
            // BETWEEN: expr >= low AND expr <= high
            Ok(Expr::BinaryOp(
                Box::new(Expr::BinaryOp(Box::new(expr.clone()), BinOp::Gte, Box::new(low))),
                BinOp::And,
                Box::new(Expr::BinaryOp(Box::new(expr), BinOp::Lte, Box::new(high))),
            ))
        }
    }

    /// Parse `group .field1, .field2 [having <expr>]`.
    fn parse_group_by(&mut self) -> Result<GroupByClause, ParseError> {
        let mut keys = Vec::new();
        loop {
            match self.peek() {
                Token::DotIdent(name) => {
                    let name = name.clone();
                    self.advance();
                    keys.push(name);
                }
                _ => break,
            }
            if *self.peek() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        if keys.is_empty() {
            return Err(ParseError {
                message: "expected at least one .field after group".into(),
            });
        }
        let having = if *self.peek() == Token::Having {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(GroupByClause { keys, having })
    }

    fn parse_additive(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus  => BinOp::Add,
                Token::Minus => BinOp::Sub,
                Token::Coalesce => {
                    self.advance();
                    let right = self.parse_multiplicative()?;
                    left = Expr::Coalesce(Box::new(left), Box::new(right));
                    continue;
                }
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expr::BinaryOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_primary()?;
        loop {
            let op = match self.peek() {
                Token::Star  => BinOp::Mul,
                Token::Slash => BinOp::Div,
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
                    // `not exists (Q)` → ExistsSubquery{ negated: true } when
                    // followed by `( Ident ...` (subquery form). Otherwise
                    // fall back to the scalar `is not null` unary op.
                    if let Some(sub) = self.try_parse_exists_subquery()? {
                        return Ok(Expr::ExistsSubquery {
                            subquery: Box::new(sub),
                            negated: true,
                        });
                    }
                    let expr = self.parse_primary()?;
                    Ok(Expr::UnaryOp(UnaryOp::NotExists, Box::new(expr)))
                } else {
                    let expr = self.parse_primary()?;
                    Ok(Expr::UnaryOp(UnaryOp::Not, Box::new(expr)))
                }
            }
            Token::Exists => {
                self.advance();
                // `exists (Q)` → ExistsSubquery when followed by a
                // parenthesised query. Scalar `exists .field` still parses
                // as UnaryOp::Exists for backwards compatibility.
                if let Some(sub) = self.try_parse_exists_subquery()? {
                    return Ok(Expr::ExistsSubquery {
                        subquery: Box::new(sub),
                        negated: false,
                    });
                }
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
                // `alias.field` → QualifiedField. The lexer emits `t1.name` as
                // `Ident("t1")` + `DotIdent("name")` (see lexer.rs line 30),
                // so a trailing DotIdent here means a qualified reference.
                if let Token::DotIdent(field) = self.peek().clone() {
                    self.advance();
                    return Ok(Expr::QualifiedField { qualifier: name, field });
                }
                Ok(Expr::Field(name))
            }
            // Window-only functions: row_number(), rank(), dense_rank()
            Token::RowNumber | Token::Rank | Token::DenseRank => {
                let wfunc = match self.advance() {
                    Token::RowNumber  => WindowFunc::RowNumber,
                    Token::Rank       => WindowFunc::Rank,
                    Token::DenseRank  => WindowFunc::DenseRank,
                    _ => return Err(ParseError { message: "unexpected window function token".into() }),
                };
                self.expect(&Token::LParen)?;
                self.expect(&Token::RParen)?;
                let (partition_by, order_by) = self.parse_over_clause()?;
                Ok(Expr::Window { function: wfunc, args: vec![], partition_by, order_by })
            }
            // Aggregate function calls inside expressions (projections, HAVING).
            // Top-level `count(User)` still routes through parse_aggregate_query
            // in parse_statement; this arm handles `count(.id)`, `sum(.age)`, etc.
            Token::Count | Token::Avg | Token::Sum | Token::Min | Token::Max => {
                let mut func = match self.advance() {
                    Token::Count => AggFunc::Count,
                    Token::Avg   => AggFunc::Avg,
                    Token::Sum   => AggFunc::Sum,
                    Token::Min   => AggFunc::Min,
                    Token::Max   => AggFunc::Max,
                    _ => return Err(ParseError { message: "unexpected aggregate token".into() }),
                };
                self.expect(&Token::LParen)?;
                // count(*) — count all rows including nulls
                if func == AggFunc::Count && *self.peek() == Token::Star {
                    self.advance();
                    self.expect(&Token::RParen)?;
                    // Check for OVER — count(*) over (...)
                    if *self.peek() == Token::Over {
                        let (partition_by, order_by) = self.parse_over_clause()?;
                        return Ok(Expr::Window {
                            function: WindowFunc::Count,
                            args: vec![Expr::Field("*".into())],
                            partition_by,
                            order_by,
                        });
                    }
                    return Ok(Expr::FunctionCall(AggFunc::Count, Box::new(Expr::Field("*".into()))));
                }
                // count(distinct .field) → CountDistinct
                if func == AggFunc::Count && *self.peek() == Token::Distinct {
                    self.advance();
                    func = AggFunc::CountDistinct;
                }
                let inner = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                // Check for OVER — e.g. sum(.salary) over (...)
                if *self.peek() == Token::Over {
                    let wfunc = match func {
                        AggFunc::Count => WindowFunc::Count,
                        AggFunc::Avg   => WindowFunc::Avg,
                        AggFunc::Sum   => WindowFunc::Sum,
                        AggFunc::Min   => WindowFunc::Min,
                        AggFunc::Max   => WindowFunc::Max,
                        _ => return Err(ParseError {
                            message: "count(distinct ...) over (...) is not supported".into(),
                        }),
                    };
                    let (partition_by, order_by) = self.parse_over_clause()?;
                    return Ok(Expr::Window { function: wfunc, args: vec![inner], partition_by, order_by });
                }
                Ok(Expr::FunctionCall(func, Box::new(inner)))
            }
            Token::Upper | Token::Lower | Token::Length | Token::Trim
            | Token::Substring | Token::Concat => {
                let func = match self.advance() {
                    Token::Upper => ScalarFn::Upper,
                    Token::Lower => ScalarFn::Lower,
                    Token::Length => ScalarFn::Length,
                    Token::Trim => ScalarFn::Trim,
                    Token::Substring => ScalarFn::Substring,
                    Token::Concat => ScalarFn::Concat,
                    _ => unreachable!(),
                };
                self.expect(&Token::LParen)?;
                let mut args = Vec::new();
                while *self.peek() != Token::RParen {
                    args.push(self.parse_expr()?);
                    if *self.peek() == Token::Comma { self.advance(); }
                }
                self.expect(&Token::RParen)?;
                Ok(Expr::ScalarFunc(func, args))
            }
            Token::Case => {
                self.advance();
                let mut whens = Vec::new();
                while *self.peek() == Token::When {
                    self.advance();
                    let condition = self.parse_expr()?;
                    self.expect(&Token::Then)?;
                    let result = self.parse_expr()?;
                    whens.push((Box::new(condition), Box::new(result)));
                }
                let else_expr = if *self.peek() == Token::Else {
                    self.advance();
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                self.expect(&Token::End)?;
                Ok(Expr::Case { whens, else_expr })
            }
            t => Err(ParseError { message: format!("unexpected token in expression: {t:?}") }),
        }
    }

    /// `alter <Table> add [column] [required] <name>: <type>`
    /// `alter <Table> drop [column] <name>`
    fn parse_alter_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Alter)?;
        let table = match self.advance() {
            Token::Ident(name) => name,
            t => return Err(ParseError { message: format!("expected table name after alter, got {t:?}") }),
        };
        match self.peek() {
            Token::Add => {
                self.advance();
                // optional `column` keyword
                if *self.peek() == Token::Column { self.advance(); }
                let required = if *self.peek() == Token::Required {
                    self.advance();
                    true
                } else {
                    false
                };
                let name = match self.advance() {
                    Token::Ident(n) => n,
                    t => return Err(ParseError { message: format!("expected column name, got {t:?}") }),
                };
                self.expect(&Token::Colon)?;
                let type_name = match self.advance() {
                    Token::Ident(n) => n,
                    t => return Err(ParseError { message: format!("expected type name, got {t:?}") }),
                };
                Ok(Statement::AlterTable(AlterTableExpr {
                    table,
                    action: AlterAction::AddColumn { name, type_name, required },
                }))
            }
            Token::Drop => {
                self.advance();
                // optional `column` keyword
                if *self.peek() == Token::Column { self.advance(); }
                let name = match self.advance() {
                    Token::Ident(n) => n,
                    t => return Err(ParseError { message: format!("expected column name, got {t:?}") }),
                };
                Ok(Statement::AlterTable(AlterTableExpr {
                    table,
                    action: AlterAction::DropColumn { name },
                }))
            }
            t => Err(ParseError { message: format!("expected add or drop after alter <table>, got {t:?}") }),
        }
    }

    /// `drop <Table>` or `drop view <ViewName>`
    fn parse_drop_or_drop_view(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Drop)?;
        if *self.peek() == Token::View {
            self.advance(); // consume `view`
            let name = match self.advance() {
                Token::Ident(name) => name,
                t => return Err(ParseError { message: format!("expected view name after drop view, got {t:?}") }),
            };
            return Ok(Statement::DropView(DropViewExpr { name }));
        }
        let table = match self.advance() {
            Token::Ident(name) => name,
            t => return Err(ParseError { message: format!("expected table name after drop, got {t:?}") }),
        };
        Ok(Statement::DropTable(DropTableExpr { table }))
    }

    /// `materialize <ViewName> as <Query>`
    ///
    /// The source query text is captured by slicing the original token stream
    /// from the position after `as` to the end.
    fn parse_create_view(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Materialized)?;
        let name = match self.advance() {
            Token::Ident(name) => name,
            t => return Err(ParseError { message: format!("expected view name after materialize, got {t:?}") }),
        };
        self.expect(&Token::As)?;
        // Record position so we can reconstruct the query text for storage.
        let query_start = self.pos;
        let source = match self.advance() {
            Token::Ident(s) => s,
            t => return Err(ParseError { message: format!("expected source table name, got {t:?}") }),
        };
        let query = self.parse_query_tail(source)?;
        // Reconstruct query text from tokens for storage and re-execution.
        let query_text = tokens_to_text(&self.tokens[query_start..self.pos]);
        Ok(Statement::CreateView(CreateViewExpr { name, query, query_text }))
    }

    /// Check for `union [all]` after a query and build a left-associative
    /// chain if present.
    fn maybe_parse_union(&mut self, left: Statement) -> Result<Statement, ParseError> {
        if *self.peek() != Token::Union {
            return Ok(left);
        }
        if !matches!(left, Statement::Query(_) | Statement::Union(_)) {
            return Err(ParseError { message: "UNION requires a query on the left side".into() });
        }
        self.advance(); // consume `union`
        let all = if let Token::Ident(s) = self.peek() {
            if s == "all" {
                self.advance();
                true
            } else {
                false
            }
        } else {
            false
        };
        // Parse the RHS as a single query (not chained — we'll chain ourselves).
        let right = self.parse_single_query()?;
        let union = Statement::Union(UnionExpr {
            left: Box::new(left),
            right: Box::new(right),
            all,
        });
        // Recursively check for further chaining: `A union B union C`
        self.maybe_parse_union(union)
    }

    /// Parse a single query statement (no UNION chaining). Used for UNION RHS.
    fn parse_single_query(&mut self) -> Result<Statement, ParseError> {
        match self.peek() {
            Token::Count | Token::Avg | Token::Sum | Token::Min | Token::Max => {
                self.parse_aggregate_query()
            }
            Token::Ident(_) => self.parse_query_or_mutation(),
            _ => Err(ParseError { message: format!("expected query after UNION, got {:?}", self.peek()) }),
        }
    }

    /// `refresh <ViewName>`
    fn parse_refresh_view(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Refresh)?;
        let name = match self.advance() {
            Token::Ident(name) => name,
            t => return Err(ParseError { message: format!("expected view name after refresh, got {t:?}") }),
        };
        Ok(Statement::RefreshView(RefreshViewExpr { name }))
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

/// Reconstruct PowQL source text from a slice of tokens. Used to store the
/// view's source query for re-execution on refresh. Not perfectly
/// round-trippable (whitespace is normalised) but semantically identical.
fn tokens_to_text(tokens: &[Token]) -> String {
    let mut out = String::with_capacity(64);
    for tok in tokens {
        if !out.is_empty() && !matches!(tok, Token::Eof) {
            out.push(' ');
        }
        match tok {
            Token::Ident(s) => out.push_str(s),
            Token::DotIdent(s) => { out.push('.'); out.push_str(s); }
            Token::IntLit(v) => out.push_str(&v.to_string()),
            Token::FloatLit(v) => out.push_str(&v.to_string()),
            Token::StringLit(s) => { out.push('"'); out.push_str(s); out.push('"'); }
            Token::BoolLit(v) => out.push_str(if *v { "true" } else { "false" }),
            Token::Param(s) => { out.push('$'); out.push_str(s); }
            Token::Type => out.push_str("type"),
            Token::Filter => out.push_str("filter"),
            Token::Order => out.push_str("order"),
            Token::Limit => out.push_str("limit"),
            Token::Offset => out.push_str("offset"),
            Token::Insert => out.push_str("insert"),
            Token::Update => out.push_str("update"),
            Token::Delete => out.push_str("delete"),
            Token::Upsert => out.push_str("upsert"),
            Token::Select => out.push_str("select"),
            Token::Required => out.push_str("required"),
            Token::Multi => out.push_str("multi"),
            Token::Link => out.push_str("link"),
            Token::Index => out.push_str("index"),
            Token::On => out.push_str("on"),
            Token::Asc => out.push_str("asc"),
            Token::Desc => out.push_str("desc"),
            Token::And => out.push_str("and"),
            Token::Or => out.push_str("or"),
            Token::Not => out.push_str("not"),
            Token::Exists => out.push_str("exists"),
            Token::Let => out.push_str("let"),
            Token::As => out.push_str("as"),
            Token::Match => out.push_str("match"),
            Token::Group => out.push_str("group"),
            Token::Join => out.push_str("join"),
            Token::Inner => out.push_str("inner"),
            Token::LeftKw => out.push_str("left"),
            Token::RightKw => out.push_str("right"),
            Token::Outer => out.push_str("outer"),
            Token::Cross => out.push_str("cross"),
            Token::Transaction => out.push_str("transaction"),
            Token::View => out.push_str("view"),
            Token::Materialized => out.push_str("materialized"),
            Token::Refresh => out.push_str("refresh"),
            Token::Union => out.push_str("union"),
            Token::Having => out.push_str("having"),
            Token::Distinct => out.push_str("distinct"),
            Token::In => out.push_str("in"),
            Token::Between => out.push_str("between"),
            Token::Like => out.push_str("like"),
            Token::Count => out.push_str("count"),
            Token::Avg => out.push_str("avg"),
            Token::Sum => out.push_str("sum"),
            Token::Min => out.push_str("min"),
            Token::Max => out.push_str("max"),
            Token::Is => out.push_str("is"),
            Token::Null => out.push_str("null"),
            Token::Upper => out.push_str("upper"),
            Token::Lower => out.push_str("lower"),
            Token::Length => out.push_str("length"),
            Token::Trim => out.push_str("trim"),
            Token::Substring => out.push_str("substring"),
            Token::Concat => out.push_str("concat"),
            Token::Case => out.push_str("case"),
            Token::When => out.push_str("when"),
            Token::Then => out.push_str("then"),
            Token::Else => out.push_str("else"),
            Token::End => out.push_str("end"),
            Token::Over => out.push_str("over"),
            Token::Partition => out.push_str("partition"),
            Token::RowNumber => out.push_str("row_number"),
            Token::Rank => out.push_str("rank"),
            Token::DenseRank => out.push_str("dense_rank"),
            Token::Alter => out.push_str("alter"),
            Token::Drop => out.push_str("drop"),
            Token::Add => out.push_str("add"),
            Token::Column => out.push_str("column"),
            Token::Eq => out.push('='),
            Token::Neq => out.push_str("!="),
            Token::Lt => out.push('<'),
            Token::Gt => out.push('>'),
            Token::Lte => out.push_str("<="),
            Token::Gte => out.push_str(">="),
            Token::Assign => out.push_str(":="),
            Token::Arrow => out.push_str("->"),
            Token::Pipe => out.push('|'),
            Token::Coalesce => out.push_str("??"),
            Token::Plus => out.push('+'),
            Token::Minus => out.push('-'),
            Token::Star => out.push('*'),
            Token::Slash => out.push('/'),
            Token::LBrace => out.push('{'),
            Token::RBrace => out.push('}'),
            Token::LParen => out.push('('),
            Token::RParen => out.push(')'),
            Token::Comma => out.push(','),
            Token::Colon => out.push(':'),
            Token::Dot => out.push('.'),
            Token::Eof => {}
        }
    }
    out
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
                assert_eq!(order.keys.len(), 1);
                assert_eq!(order.keys[0].field, "name");
                assert!(order.keys[0].descending);
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

    // ---- Mission E1.1: JOIN parser tests ----------------------------------
    // Parser-level only. The planner rejects joins with a clean error until
    // E1.2 wires up execution.

    #[test]
    fn test_parse_source_alias() {
        let stmt = parse("User as u filter u.age > 30").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.source, "User");
                assert_eq!(q.alias.as_deref(), Some("u"));
                assert!(q.joins.is_empty());
                match q.filter.unwrap() {
                    Expr::BinaryOp(l, BinOp::Gt, _) => match *l {
                        Expr::QualifiedField { qualifier, field } => {
                            assert_eq!(qualifier, "u");
                            assert_eq!(field, "age");
                        }
                        other => panic!("expected qualified field, got {other:?}"),
                    },
                    other => panic!("expected >, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_inner_join_on() {
        let stmt = parse("User as u inner join Order as o on u.id = o.user_id").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.source, "User");
                assert_eq!(q.alias.as_deref(), Some("u"));
                assert_eq!(q.joins.len(), 1);
                let j = &q.joins[0];
                assert_eq!(j.kind, JoinKind::Inner);
                assert_eq!(j.source, "Order");
                assert_eq!(j.alias.as_deref(), Some("o"));
                let on = j.on.as_ref().expect("on clause");
                match on {
                    Expr::BinaryOp(l, BinOp::Eq, r) => {
                        assert!(matches!(**l, Expr::QualifiedField { .. }));
                        assert!(matches!(**r, Expr::QualifiedField { .. }));
                    }
                    other => panic!("expected eq, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_bare_join_defaults_to_inner() {
        let stmt = parse("User join Order on User.id = Order.user_id").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.joins.len(), 1);
                assert_eq!(q.joins[0].kind, JoinKind::Inner);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_left_outer_join() {
        let stmt = parse("User as u left outer join Order as o on u.id = o.user_id").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.joins.len(), 1);
                assert_eq!(q.joins[0].kind, JoinKind::LeftOuter);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_left_join_without_outer_keyword() {
        // `left join` is shorthand for `left outer join` in SQL — we accept it.
        let stmt = parse("User as u left join Order as o on u.id = o.user_id").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.joins[0].kind, JoinKind::LeftOuter);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_right_join() {
        let stmt = parse("User as u right join Order as o on u.id = o.user_id").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.joins[0].kind, JoinKind::RightOuter);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_cross_join_has_no_on() {
        let stmt = parse("User cross join Order").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.joins[0].kind, JoinKind::Cross);
                assert!(q.joins[0].on.is_none());
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_multi_join_chain() {
        let stmt = parse(
            "User as u join Order as o on u.id = o.user_id \
             join Product as p on o.product_id = p.id",
        )
        .unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.joins.len(), 2);
                assert_eq!(q.joins[0].source, "Order");
                assert_eq!(q.joins[1].source, "Product");
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_join_with_filter_tail() {
        // Filter/order/limit still work after a join clause.
        let stmt = parse(
            "User as u join Order as o on u.id = o.user_id \
             filter o.total > 100 order .name limit 10",
        )
        .unwrap();
        match stmt {
            Statement::Query(q) => {
                assert_eq!(q.joins.len(), 1);
                assert!(q.filter.is_some());
                assert!(q.order.is_some());
                assert!(q.limit.is_some());
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_join_requires_on_for_inner() {
        // Non-cross joins require `on <expr>`. Missing `on` is a parse error.
        let err = parse("User join Order").unwrap_err();
        assert!(
            err.message.contains("on"),
            "expected on-clause error, got {:?}",
            err.message
        );
    }

    #[test]
    fn test_parse_update_on_joined_query_errors() {
        // E1.1 explicitly rejects update/delete on joined queries — SQL
        // semantics here are messy and we're not implementing them yet.
        let err = parse("User as u join Order as o on u.id = o.user_id update { age := 1 }")
            .unwrap_err();
        assert!(err.message.contains("update"));
    }

    #[test]
    fn test_parse_delete_on_joined_query_errors() {
        let err =
            parse("User as u join Order as o on u.id = o.user_id delete").unwrap_err();
        assert!(err.message.contains("delete"));
    }

    // ---- Mission E2a: DISTINCT + IN-list + BETWEEN + LIKE -----------------

    #[test]
    fn test_parse_distinct() {
        let stmt = parse("User distinct { .name }").unwrap();
        match stmt {
            Statement::Query(q) => {
                assert!(q.distinct);
                assert!(q.projection.is_some());
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_in_list() {
        let stmt = parse(r#"User filter .name in ("Alice", "Bob")"#).unwrap();
        match stmt {
            Statement::Query(q) => {
                match q.filter.unwrap() {
                    Expr::InList { expr, list, negated } => {
                        assert!(!negated);
                        assert!(matches!(*expr, Expr::Field(f) if f == "name"));
                        assert_eq!(list.len(), 2);
                    }
                    other => panic!("expected InList, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_not_in_list() {
        let stmt = parse("User filter .age not in (1, 2, 3)").unwrap();
        match stmt {
            Statement::Query(q) => {
                match q.filter.unwrap() {
                    Expr::InList { negated, list, .. } => {
                        assert!(negated);
                        assert_eq!(list.len(), 3);
                    }
                    other => panic!("expected InList, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_between() {
        // BETWEEN desugars into >= AND <=.
        let stmt = parse("User filter .age between 10 and 20").unwrap();
        match stmt {
            Statement::Query(q) => {
                match q.filter.unwrap() {
                    Expr::BinaryOp(_, BinOp::And, _) => {} // desugared
                    other => panic!("expected And (desugared between), got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_not_between() {
        // NOT BETWEEN desugars into < OR >.
        let stmt = parse("User filter .age not between 10 and 20").unwrap();
        match stmt {
            Statement::Query(q) => {
                match q.filter.unwrap() {
                    Expr::BinaryOp(_, BinOp::Or, _) => {} // desugared
                    other => panic!("expected Or (desugared not between), got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_like() {
        let stmt = parse(r#"User filter .name like "A%""#).unwrap();
        match stmt {
            Statement::Query(q) => {
                match q.filter.unwrap() {
                    Expr::BinaryOp(l, BinOp::Like, r) => {
                        assert!(matches!(*l, Expr::Field(f) if f == "name"));
                        assert!(matches!(*r, Expr::Literal(Literal::String(s)) if s == "A%"));
                    }
                    other => panic!("expected Like, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_not_like() {
        let stmt = parse(r#"User filter .name not like "A%""#).unwrap();
        match stmt {
            Statement::Query(q) => {
                match q.filter.unwrap() {
                    Expr::UnaryOp(UnaryOp::Not, inner) => {
                        assert!(matches!(*inner, Expr::BinaryOp(_, BinOp::Like, _)));
                    }
                    other => panic!("expected Not(Like), got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    // ---- Mission E2b: GROUP BY + HAVING ------------------------------------

    #[test]
    fn test_parse_group_by_single_key() {
        let stmt = parse("User group .status { .status, n: count(.name) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let gb = q.group_by.unwrap();
                assert_eq!(gb.keys, vec!["status"]);
                assert!(gb.having.is_none());
                let proj = q.projection.unwrap();
                assert_eq!(proj.len(), 2);
                assert!(matches!(&proj[1].expr, Expr::FunctionCall(AggFunc::Count, _)));
                assert_eq!(proj[1].alias.as_deref(), Some("n"));
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_group_by_multi_key() {
        let stmt = parse("User group .status, .age { .status, .age }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let gb = q.group_by.unwrap();
                assert_eq!(gb.keys, vec!["status", "age"]);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_group_by_having() {
        let stmt = parse("User group .status having count(.name) > 1 { .status }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let gb = q.group_by.unwrap();
                assert_eq!(gb.keys, vec!["status"]);
                assert!(gb.having.is_some());
                // HAVING is `count(.name) > 1` — BinaryOp(FunctionCall, Gt, Literal)
                match gb.having.unwrap() {
                    Expr::BinaryOp(l, BinOp::Gt, _) => {
                        assert!(matches!(*l, Expr::FunctionCall(AggFunc::Count, _)));
                    }
                    other => panic!("expected BinaryOp, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_aggregate_in_projection() {
        // Unaliased aggregate function calls in projection.
        let stmt = parse("User group .status { .status, count(.name), sum(.age) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj.len(), 3);
                assert!(matches!(&proj[1].expr, Expr::FunctionCall(AggFunc::Count, _)));
                assert!(matches!(&proj[2].expr, Expr::FunctionCall(AggFunc::Sum, _)));
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_aggregate_in_aliased_projection() {
        let stmt = parse("User group .status { .status, total: count(.name), average: avg(.age) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj[1].alias.as_deref(), Some("total"));
                assert!(matches!(&proj[1].expr, Expr::FunctionCall(AggFunc::Count, _)));
                assert_eq!(proj[2].alias.as_deref(), Some("average"));
                assert!(matches!(&proj[2].expr, Expr::FunctionCall(AggFunc::Avg, _)));
            }
            _ => panic!("expected query"),
        }
    }

    // ─── IS NULL / IS NOT NULL parser tests ────────────────────────────

    #[test]
    fn test_parse_is_null() {
        let stmt = parse("User filter .age is null").unwrap();
        match stmt {
            Statement::Query(q) => {
                let filter = q.filter.unwrap();
                assert_eq!(
                    filter,
                    Expr::UnaryOp(UnaryOp::IsNull, Box::new(Expr::Field("age".into())))
                );
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_is_not_null() {
        let stmt = parse("User filter .age is not null").unwrap();
        match stmt {
            Statement::Query(q) => {
                let filter = q.filter.unwrap();
                assert_eq!(
                    filter,
                    Expr::UnaryOp(UnaryOp::IsNotNull, Box::new(Expr::Field("age".into())))
                );
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_count_star_expr() {
        let stmt = parse("User filter count(*) > 0").unwrap();
        match stmt {
            Statement::Query(q) => {
                let filter = q.filter.unwrap();
                match filter {
                    Expr::BinaryOp(left, BinOp::Gt, _) => {
                        assert_eq!(
                            *left,
                            Expr::FunctionCall(AggFunc::Count, Box::new(Expr::Field("*".into())))
                        );
                    }
                    _ => panic!("expected comparison"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    // ─── String function parser tests ──────────────────────────────────

    #[test]
    fn test_parse_upper_in_filter() {
        let stmt = parse(r#"User filter upper(.name) = "ALICE""#).unwrap();
        match stmt {
            Statement::Query(q) => {
                let f = q.filter.unwrap();
                match f {
                    Expr::BinaryOp(left, BinOp::Eq, _right) => {
                        assert!(matches!(*left, Expr::ScalarFunc(ScalarFn::Upper, _)));
                    }
                    _ => panic!("expected binary op with upper"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_substring() {
        let stmt = parse("User { sub: substring(.name, 1, 3) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                match &proj[0].expr {
                    Expr::ScalarFunc(ScalarFn::Substring, args) => {
                        assert_eq!(args.len(), 3);
                    }
                    other => panic!("expected ScalarFunc Substring, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_concat() {
        let stmt = parse(r#"User { full: concat(.name, " - ", .email) }"#).unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                match &proj[0].expr {
                    Expr::ScalarFunc(ScalarFn::Concat, args) => {
                        assert_eq!(args.len(), 3);
                    }
                    other => panic!("expected ScalarFunc Concat, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    // ─── CASE WHEN parser tests ────────────────────────────────────────

    #[test]
    fn test_parse_case_single_when() {
        let stmt = parse(r#"User filter case when .age > 30 then true else false end"#).unwrap();
        match stmt {
            Statement::Query(q) => {
                let filter = q.filter.unwrap();
                match filter {
                    Expr::Case { whens, else_expr } => {
                        assert_eq!(whens.len(), 1);
                        assert!(else_expr.is_some());
                    }
                    other => panic!("expected Case expr, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_case_multiple_whens() {
        let stmt = parse(
            r#"User { label: case when .age > 30 then "senior" when .age > 20 then "adult" else "young" end }"#
        ).unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                match &proj[0].expr {
                    Expr::Case { whens, else_expr } => {
                        assert_eq!(whens.len(), 2);
                        assert!(else_expr.is_some());
                    }
                    other => panic!("expected Case expr, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_case_without_else() {
        let stmt = parse(r#"User filter case when .age > 30 then true end"#).unwrap();
        match stmt {
            Statement::Query(q) => {
                let filter = q.filter.unwrap();
                match filter {
                    Expr::Case { whens, else_expr } => {
                        assert_eq!(whens.len(), 1);
                        assert!(else_expr.is_none());
                    }
                    other => panic!("expected Case expr, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    // ─── Mul/Div expression tests (E2f) ───────────────────────────────

    #[test]
    fn test_parse_mul_expr() {
        let stmt = parse("User filter .price * .quantity > 100").unwrap();
        match stmt {
            Statement::Query(q) => {
                let filter = q.filter.unwrap();
                match filter {
                    Expr::BinaryOp(left, BinOp::Gt, _) => {
                        match *left {
                            Expr::BinaryOp(_, BinOp::Mul, _) => {}
                            other => panic!("expected Mul, got {other:?}"),
                        }
                    }
                    other => panic!("expected BinaryOp Gt, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_div_expr() {
        let stmt = parse("User { ratio: .total / .count }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj[0].alias.as_deref(), Some("ratio"));
                match &proj[0].expr {
                    Expr::BinaryOp(_, BinOp::Div, _) => {}
                    other => panic!("expected Div, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_mul_div_precedence() {
        // .a + .b * .c should parse as .a + (.b * .c)
        let stmt = parse("User filter .a + .b * .c > 0").unwrap();
        match stmt {
            Statement::Query(q) => {
                let filter = q.filter.unwrap();
                match filter {
                    Expr::BinaryOp(left, BinOp::Gt, _) => {
                        match *left {
                            Expr::BinaryOp(_, BinOp::Add, right) => {
                                assert!(matches!(*right, Expr::BinaryOp(_, BinOp::Mul, _)));
                            }
                            other => panic!("expected Add, got {other:?}"),
                        }
                    }
                    other => panic!("expected Gt, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    // ─── Multi-column ORDER BY tests (E2f) ────────────────────────────

    #[test]
    fn test_parse_multi_order() {
        let stmt = parse("User order .name asc, .age desc").unwrap();
        match stmt {
            Statement::Query(q) => {
                let order = q.order.unwrap();
                assert_eq!(order.keys.len(), 2);
                assert_eq!(order.keys[0].field, "name");
                assert!(!order.keys[0].descending);
                assert_eq!(order.keys[1].field, "age");
                assert!(order.keys[1].descending);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_order_default_asc() {
        let stmt = parse("User order .name").unwrap();
        match stmt {
            Statement::Query(q) => {
                let order = q.order.unwrap();
                assert_eq!(order.keys.len(), 1);
                assert!(!order.keys[0].descending);
            }
            _ => panic!("expected query"),
        }
    }

    // ─── ALTER TABLE / DROP TABLE parser tests (E2g) ──────────────────

    #[test]
    fn test_parse_alter_add_column() {
        let stmt = parse("alter User add column status: str").unwrap();
        match stmt {
            Statement::AlterTable(at) => {
                assert_eq!(at.table, "User");
                match at.action {
                    AlterAction::AddColumn { name, type_name, required } => {
                        assert_eq!(name, "status");
                        assert_eq!(type_name, "str");
                        assert!(!required);
                    }
                    other => panic!("expected AddColumn, got {other:?}"),
                }
            }
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_alter_add_required_column() {
        let stmt = parse("alter User add required status: str").unwrap();
        match stmt {
            Statement::AlterTable(at) => {
                match at.action {
                    AlterAction::AddColumn { required, .. } => assert!(required),
                    other => panic!("expected AddColumn, got {other:?}"),
                }
            }
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_alter_drop_column() {
        let stmt = parse("alter User drop column status").unwrap();
        match stmt {
            Statement::AlterTable(at) => {
                assert_eq!(at.table, "User");
                match at.action {
                    AlterAction::DropColumn { name } => assert_eq!(name, "status"),
                    other => panic!("expected DropColumn, got {other:?}"),
                }
            }
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_alter_drop_without_column_keyword() {
        let stmt = parse("alter User drop status").unwrap();
        match stmt {
            Statement::AlterTable(at) => {
                match at.action {
                    AlterAction::DropColumn { name } => assert_eq!(name, "status"),
                    other => panic!("expected DropColumn, got {other:?}"),
                }
            }
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = parse("drop User").unwrap();
        match stmt {
            Statement::DropTable(dt) => assert_eq!(dt.table, "User"),
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    // ─── IN subquery parser tests (E2h) ───────────────────────────────

    #[test]
    fn test_parse_in_subquery() {
        let stmt = parse("User filter .name in (VIP { .name })").unwrap();
        match stmt {
            Statement::Query(q) => {
                let filter = q.filter.unwrap();
                match filter {
                    Expr::InSubquery { expr, subquery, negated } => {
                        assert!(!negated);
                        assert!(matches!(*expr, Expr::Field(ref f) if f == "name"));
                        assert_eq!(subquery.source, "VIP");
                    }
                    other => panic!("expected InSubquery, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_not_in_subquery() {
        let stmt = parse("User filter .id not in (Order { .user_id })").unwrap();
        match stmt {
            Statement::Query(q) => {
                match q.filter.unwrap() {
                    Expr::InSubquery { negated, .. } => assert!(negated),
                    other => panic!("expected InSubquery, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_in_literal_list_still_works() {
        // Ensure existing IN (literal) parsing isn't broken
        let stmt = parse("User filter .age in (25, 30, 35)").unwrap();
        match stmt {
            Statement::Query(q) => {
                match q.filter.unwrap() {
                    Expr::InList { list, negated, .. } => {
                        assert!(!negated);
                        assert_eq!(list.len(), 3);
                    }
                    other => panic!("expected InList, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    // ---- Materialized view parser tests ------------------------------------

    #[test]
    fn test_parse_create_view() {
        let stmt = parse("materialize OldUsers as User filter .age > 28").unwrap();
        match stmt {
            Statement::CreateView(cv) => {
                assert_eq!(cv.name, "OldUsers");
                assert_eq!(cv.query.source, "User");
                assert!(cv.query.filter.is_some());
                assert!(!cv.query_text.is_empty());
            }
            _ => panic!("expected CreateView"),
        }
    }

    #[test]
    fn test_parse_create_view_with_projection() {
        let stmt = parse("materialize UserNames as User { .name }").unwrap();
        match stmt {
            Statement::CreateView(cv) => {
                assert_eq!(cv.name, "UserNames");
                assert!(cv.query.projection.is_some());
            }
            _ => panic!("expected CreateView"),
        }
    }

    #[test]
    fn test_parse_refresh_view() {
        let stmt = parse("refresh OldUsers").unwrap();
        match stmt {
            Statement::RefreshView(rv) => {
                assert_eq!(rv.name, "OldUsers");
            }
            _ => panic!("expected RefreshView"),
        }
    }

    #[test]
    fn test_parse_drop_view() {
        let stmt = parse("drop view OldUsers").unwrap();
        match stmt {
            Statement::DropView(dv) => {
                assert_eq!(dv.name, "OldUsers");
            }
            _ => panic!("expected DropView"),
        }
    }

    #[test]
    fn test_parse_drop_table_still_works() {
        let stmt = parse("drop Users").unwrap();
        match stmt {
            Statement::DropTable(dt) => {
                assert_eq!(dt.table, "Users");
            }
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn test_parse_union() {
        let stmt = parse("User union Order").unwrap();
        match stmt {
            Statement::Union(u) => {
                assert!(!u.all);
                match *u.left {
                    Statement::Query(_) => {}
                    _ => panic!("expected Query on left"),
                }
                match *u.right {
                    Statement::Query(_) => {}
                    _ => panic!("expected Query on right"),
                }
            }
            _ => panic!("expected Union"),
        }
    }

    #[test]
    fn test_parse_union_all() {
        let stmt = parse("User union all Order").unwrap();
        match stmt {
            Statement::Union(u) => {
                assert!(u.all, "expected UNION ALL");
                match *u.left {
                    Statement::Query(_) => {}
                    _ => panic!("expected Query on left"),
                }
                match *u.right {
                    Statement::Query(_) => {}
                    _ => panic!("expected Query on right"),
                }
            }
            _ => panic!("expected Union"),
        }
    }

    #[test]
    fn test_parse_union_chain() {
        // Left-associative: A union B union C => Union(Union(A, B), C)
        let stmt = parse("User union Order union Product").unwrap();
        match stmt {
            Statement::Union(outer) => {
                assert!(!outer.all);
                // Right side is Product
                match *outer.right {
                    Statement::Query(q) => assert_eq!(q.source, "Product"),
                    _ => panic!("expected Query(Product) on right"),
                }
                // Left side is Union(User, Order)
                match *outer.left {
                    Statement::Union(inner) => {
                        assert!(!inner.all);
                        match *inner.left {
                            Statement::Query(q) => assert_eq!(q.source, "User"),
                            _ => panic!("expected Query(User)"),
                        }
                        match *inner.right {
                            Statement::Query(q) => assert_eq!(q.source, "Order"),
                            _ => panic!("expected Query(Order)"),
                        }
                    }
                    _ => panic!("expected inner Union"),
                }
            }
            _ => panic!("expected Union"),
        }
    }

    #[test]
    fn test_parse_union_with_filter() {
        let stmt = parse("User filter .age > 10 union Order filter .total > 50").unwrap();
        match stmt {
            Statement::Union(u) => {
                assert!(!u.all);
                // Both sides should be queries (the filter is part of each query)
                match *u.left {
                    Statement::Query(q) => {
                        assert_eq!(q.source, "User");
                        assert!(q.filter.is_some());
                    }
                    _ => panic!("expected Query on left"),
                }
                match *u.right {
                    Statement::Query(q) => {
                        assert_eq!(q.source, "Order");
                        assert!(q.filter.is_some());
                    }
                    _ => panic!("expected Query on right"),
                }
            }
            _ => panic!("expected Union"),
        }
    }

    #[test]
    fn test_parse_count_distinct_standalone() {
        let stmt = parse("count(distinct User { .name })").unwrap();
        match stmt {
            Statement::Query(q) => {
                let agg = q.aggregation.unwrap();
                assert_eq!(agg.function, AggFunc::CountDistinct);
                assert_eq!(agg.field.as_deref(), Some("name"));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_count_distinct_in_projection() {
        let stmt = parse("User group .dept { .dept, count(distinct .name) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj.len(), 2);
                match &proj[1].expr {
                    Expr::FunctionCall(func, _) => {
                        assert_eq!(*func, AggFunc::CountDistinct);
                    }
                    _ => panic!("expected FunctionCall"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    // ---- Window function parser tests ----------------------------------------

    #[test]
    fn test_parse_window_row_number_order() {
        let stmt = parse("User { .name, rn: row_number() over (order .age) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj.len(), 2);
                assert_eq!(proj[1].alias.as_deref(), Some("rn"));
                match &proj[1].expr {
                    Expr::Window { function, args, partition_by, order_by } => {
                        assert_eq!(*function, WindowFunc::RowNumber);
                        assert!(args.is_empty());
                        assert!(partition_by.is_empty());
                        assert_eq!(order_by.len(), 1);
                        assert_eq!(order_by[0].field, "age");
                        assert!(!order_by[0].descending);
                    }
                    other => panic!("expected Window, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_window_sum_partition_order() {
        let stmt = parse("User { .name, s: sum(.salary) over (partition .dept order .salary) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj.len(), 2);
                assert_eq!(proj[1].alias.as_deref(), Some("s"));
                match &proj[1].expr {
                    Expr::Window { function, args, partition_by, order_by } => {
                        assert_eq!(*function, WindowFunc::Sum);
                        assert_eq!(args.len(), 1);
                        assert!(matches!(&args[0], Expr::Field(f) if f == "salary"));
                        assert_eq!(partition_by, &["dept"]);
                        assert_eq!(order_by.len(), 1);
                        assert_eq!(order_by[0].field, "salary");
                        assert!(!order_by[0].descending);
                    }
                    other => panic!("expected Window, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_window_rank_desc() {
        let stmt = parse("User { .dept, .salary, r: rank() over (partition .dept order .salary desc) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj.len(), 3);
                match &proj[2].expr {
                    Expr::Window { function, partition_by, order_by, .. } => {
                        assert_eq!(*function, WindowFunc::Rank);
                        assert_eq!(partition_by, &["dept"]);
                        assert_eq!(order_by.len(), 1);
                        assert!(order_by[0].descending);
                    }
                    other => panic!("expected Window, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_window_dense_rank() {
        let stmt = parse("User { .name, dr: dense_rank() over (order .score desc) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj.len(), 2);
                match &proj[1].expr {
                    Expr::Window { function, .. } => {
                        assert_eq!(*function, WindowFunc::DenseRank);
                    }
                    other => panic!("expected Window, got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_sum_without_over_is_aggregate() {
        // sum(.salary) alone (no `over`) stays as FunctionCall, not Window.
        let stmt = parse("User group .dept { .dept, total: sum(.salary) }").unwrap();
        match stmt {
            Statement::Query(q) => {
                let proj = q.projection.unwrap();
                assert_eq!(proj.len(), 2);
                match &proj[1].expr {
                    Expr::FunctionCall(AggFunc::Sum, _) => {} // correct
                    other => panic!("expected FunctionCall(Sum), got {other:?}"),
                }
            }
            _ => panic!("expected query"),
        }
    }
}
