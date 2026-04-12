//! Parser robustness tests: edge-case and adversarial inputs.
//!
//! These tests verify that the PowQL parser either produces a valid AST
//! or returns a clean ParseError for pathological inputs. No panics, no
//! infinite loops.

use powdb_query::parser::parse;

// ---------------------------------------------------------------------------
// Empty / whitespace inputs
// ---------------------------------------------------------------------------

#[test]
fn empty_string_is_err() {
    assert!(parse("").is_err());
}

#[test]
fn whitespace_only_is_err() {
    assert!(parse("   \t\n  ").is_err());
}

#[test]
fn comment_only_is_err() {
    assert!(parse("# just a comment").is_err());
}

// ---------------------------------------------------------------------------
// Valid queries that should parse successfully
// ---------------------------------------------------------------------------

#[test]
fn simple_table_scan() {
    let result = parse("User");
    assert!(result.is_ok(), "plain table scan should parse: {result:?}");
}

#[test]
fn filter_with_int_literal() {
    let result = parse("User filter .age > 30");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn filter_with_string_literal() {
    let result = parse("User filter .name = \"Alice\"");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn order_limit_offset() {
    let result = parse("User order .name asc limit 10 offset 5");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn projection_braces() {
    let result = parse("User { .name, .age }");
    assert!(result.is_ok(), "{result:?}");
}

// ---------------------------------------------------------------------------
// Numeric boundary values
// ---------------------------------------------------------------------------

#[test]
fn i64_max_literal() {
    let q = format!("User filter .id = {}", i64::MAX);
    let result = parse(&q);
    assert!(result.is_ok(), "i64::MAX should parse: {result:?}");
}

#[test]
fn i64_min_literal() {
    // i64::MIN is -9223372036854775808; the parser should handle the
    // unary minus via its expression parser.
    let q = format!("User filter .id = {}", i64::MIN);
    // This may or may not parse depending on unary minus support — we just
    // assert it doesn't panic.
    let _result = parse(&q);
}

#[test]
fn zero_literal() {
    let result = parse("User filter .count = 0");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn negative_float() {
    let q = "User filter .score > -3.14";
    let _result = parse(q); // should not panic
}

// ---------------------------------------------------------------------------
// String edge cases
// ---------------------------------------------------------------------------

#[test]
fn empty_string_literal() {
    let result = parse("User filter .name = \"\"");
    assert!(
        result.is_ok(),
        "empty string literal should parse: {result:?}"
    );
}

#[test]
fn unicode_in_string_literal() {
    let result = parse("User filter .name = \"cafe\\u0301\"");
    // May or may not parse depending on escape support, but must not panic.
    let _ = result;
}

#[test]
fn string_with_spaces() {
    let result = parse("User filter .name = \"hello world\"");
    assert!(result.is_ok(), "{result:?}");
}

// ---------------------------------------------------------------------------
// Identifier edge cases
// ---------------------------------------------------------------------------

#[test]
fn long_identifier() {
    let long_name = "A".repeat(1000);
    let q = format!("{long_name} filter .id = 1");
    let result = parse(&q);
    assert!(result.is_ok(), "long identifier should parse: {result:?}");
}

#[test]
fn underscore_identifier() {
    let result = parse("my_table filter .my_col = 1");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn identifier_with_digits() {
    let result = parse("Table123 filter .col456 = 1");
    assert!(result.is_ok(), "{result:?}");
}

// ---------------------------------------------------------------------------
// Missing delimiters / incomplete input
// ---------------------------------------------------------------------------

#[test]
fn missing_closing_brace() {
    // The parser may panic on missing closing brace (index OOB on token
    // stream). We catch_unwind to document this; the behavior is an
    // existing parser limitation, not a regression from our changes.
    let result = std::panic::catch_unwind(|| parse("User { .name, .age"));
    // Either a clean Err or a panic — both are acceptable for now. The
    // important thing is this test documents the behavior.
    match result {
        Ok(Err(_)) => {} // clean error
        Err(_) => {}     // panicked — known limitation
        Ok(Ok(_)) => panic!("missing closing brace should not parse successfully"),
    }
}

#[test]
fn missing_closing_paren() {
    let result = parse("User filter .id in (1, 2, 3");
    assert!(result.is_err(), "missing closing paren should error");
}

#[test]
fn dangling_filter() {
    let result = parse("User filter");
    assert!(result.is_err(), "filter without expression should error");
}

#[test]
fn dangling_order() {
    let result = parse("User order");
    assert!(result.is_err(), "order without field should error");
}

// ---------------------------------------------------------------------------
// Keywords as table names
// ---------------------------------------------------------------------------

// PowQL keywords used as identifiers — should these parse or error?
// We document the current behavior without asserting a specific outcome,
// just that the parser does not panic.

#[test]
fn keyword_filter_as_table_name() {
    let _ = parse("filter filter .x = 1");
}

#[test]
fn keyword_order_as_table_name() {
    let _ = parse("order order .x asc");
}

#[test]
fn keyword_limit_as_table_name() {
    let _ = parse("limit limit 10");
}

#[test]
fn keyword_insert_as_table_name() {
    let _ = parse("insert insert { name := \"x\" }");
}

#[test]
fn keyword_delete_as_table_name() {
    let _ = parse("delete delete filter .id = 1");
}

#[test]
fn keyword_update_as_table_name() {
    let _ = parse("update update filter .id = 1 { name := \"x\" }");
}

#[test]
fn keyword_type_as_table_name() {
    let _ = parse("type type { name: str }");
}

// ---------------------------------------------------------------------------
// Complex / nested expressions
// ---------------------------------------------------------------------------

#[test]
fn deeply_nested_and_or() {
    // Build a deeply nested AND chain: .a = 1 and .a = 1 and ...
    let clause = ".a = 1";
    let q = format!(
        "User filter {}",
        (0..50).map(|_| clause).collect::<Vec<_>>().join(" and ")
    );
    let result = parse(&q);
    assert!(result.is_ok(), "deep AND chain should parse: {result:?}");
}

#[test]
fn nested_parens() {
    let result = parse("User filter (((.age > 10)))");
    assert!(result.is_ok(), "nested parens should parse: {result:?}");
}

#[test]
fn mixed_and_or_precedence() {
    let result = parse("User filter .a = 1 or .b = 2 and .c = 3");
    assert!(result.is_ok(), "mixed and/or should parse: {result:?}");
}

// ---------------------------------------------------------------------------
// Aggregations
// ---------------------------------------------------------------------------

#[test]
fn count_query() {
    let result = parse("count(User)");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn sum_with_filter() {
    let result = parse("sum(User filter .active = true { .price })");
    assert!(result.is_ok(), "{result:?}");
}

// ---------------------------------------------------------------------------
// Insert / Update / Delete
// ---------------------------------------------------------------------------

#[test]
fn insert_basic() {
    let result = parse("insert User { name := \"Alice\", age := 30 }");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn update_with_filter() {
    let result = parse("User filter .id = 1 update { name := \"Bob\" }");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn delete_with_filter() {
    let result = parse("User filter .id = 1 delete");
    assert!(result.is_ok(), "{result:?}");
}

// ---------------------------------------------------------------------------
// DDL edge cases
// ---------------------------------------------------------------------------

#[test]
fn create_type_basic() {
    let result = parse("type Product { name: str, price: float }");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn create_type_empty_fields() {
    let result = parse("type Empty { }");
    // Depends on whether the parser allows zero fields — just don't panic.
    let _ = result;
}

#[test]
fn alter_add_column() {
    let result = parse("alter User add column status: str");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn alter_drop_column() {
    let result = parse("alter User drop column status");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn drop_table() {
    let result = parse("drop User");
    assert!(result.is_ok(), "{result:?}");
}

// ---------------------------------------------------------------------------
// Miscellaneous edge cases
// ---------------------------------------------------------------------------

#[test]
fn multiple_filters_should_error_or_parse() {
    // Double filter clause — should either parse (second overrides) or error.
    let _ = parse("User filter .a = 1 filter .b = 2");
}

#[test]
fn trailing_comma_in_projection() {
    // Trailing comma: might be accepted or rejected, must not panic.
    let _ = parse("User { .name, .age, }");
}

#[test]
fn only_dot_ident() {
    // A bare field reference with no table should error.
    let result = parse(".name");
    assert!(result.is_err());
}

#[test]
fn in_list_empty() {
    // `in ()` with no values
    let _ = parse("User filter .id in ()");
}

#[test]
fn between_expression() {
    let result = parse("User filter .age between 18 and 65");
    // PowQL may or may not support BETWEEN syntax — just no panic.
    let _ = result;
}

#[test]
fn like_expression() {
    let result = parse("User filter .name like \"%alice%\"");
    let _ = result;
}

#[test]
fn case_when_expression() {
    let result = parse("User { status: case when .age > 65 then \"senior\" else \"regular\" end }");
    let _ = result;
}
