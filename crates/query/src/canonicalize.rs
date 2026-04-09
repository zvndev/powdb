//! Mission D9: query canonicalization for the plan cache.
//!
//! Two queries that differ only in literal values share the same *shape* —
//! e.g. `User filter .id = 1` and `User filter .id = 2`. We want both to
//! hit the same cached plan and only substitute the literal values at
//! execute time. This module provides:
//!
//!   - [`canonicalize`] — lex an input query, hash the token stream with
//!     literal values *replaced by placeholders*, and collect the literal
//!     values in source order. The hash is the cache key; the literal
//!     vector is what we re-bind into the cached plan on a hit.
//!
//! The canonicalisation is **token-level**, not string-level. That means
//! whitespace and comment differences (`User filter .id=1` vs
//! `User filter .id = 1  # foo`) collapse to the same hash. It also means
//! literal extraction is unambiguous — no regex tricks, no risk of
//! confusing `42` inside a string with a real int literal.
//!
//! ## Hash format
//!
//! FNV-1a over a stream of "canonical bytes":
//!   - Every keyword and operator token contributes a unique 1-byte tag.
//!   - Identifier tokens contribute `[tag, len, bytes...]`.
//!   - Literal tokens contribute *only* their tag (`0xF0..0xF3`) — the
//!     value is collected separately into the literal list.
//!
//! The tag scheme is bespoke (not `mem::discriminant`) so the cache key is
//! stable across crate rebuilds. This matters for test reproducibility,
//! not for correctness; the cache itself is in-memory only.

use crate::ast::Literal;
use crate::lexer::{lex, LexError};
use crate::token::Token;

/// FNV-1a constants. The hash is u64 because the cache map is keyed on u64.
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

#[inline]
fn hash_byte(mut h: u64, b: u8) -> u64 {
    h ^= b as u64;
    h = h.wrapping_mul(FNV_PRIME);
    h
}

#[inline]
fn hash_bytes(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h = hash_byte(h, *b);
    }
    h
}

/// Lex `input`, hash the canonicalised token stream, and collect literal
/// values in source order.
///
/// Returns `(canonical_hash, literals)`. The hash is the cache key; on a
/// hit, [`crate::plan_cache::PlanCache::substitute_literals`] re-binds the
/// new literals into the cached plan.
pub fn canonicalize(input: &str) -> Result<(u64, Vec<Literal>), LexError> {
    let tokens = lex(input)?;
    let mut hash = FNV_OFFSET;
    // Most queries hold 0-3 literals; we pre-size to avoid allocation
    // churn on the bench's tight loops.
    let mut literals: Vec<Literal> = Vec::with_capacity(4);
    for tok in &tokens {
        hash = hash_token(hash, tok, &mut literals);
    }
    Ok((hash, literals))
}

fn hash_token(h: u64, tok: &Token, literals: &mut Vec<Literal>) -> u64 {
    match tok {
        // Identifiers — hash tag + length + bytes so two queries with
        // different identifiers (e.g. `User` vs `Order`) get different
        // hashes.
        Token::Ident(s) => {
            let h = hash_byte(h, 0x01);
            let h = hash_byte(h, s.len() as u8);
            hash_bytes(h, s.as_bytes())
        }
        Token::DotIdent(s) => {
            let h = hash_byte(h, 0x02);
            let h = hash_byte(h, s.len() as u8);
            hash_bytes(h, s.as_bytes())
        }
        Token::Param(s) => {
            let h = hash_byte(h, 0x03);
            let h = hash_byte(h, s.len() as u8);
            hash_bytes(h, s.as_bytes())
        }

        // Literals — hash *only* the tag, collect value separately. This
        // is the whole point of the module: 100 calls with 100 different
        // literal values produce 1 hash, 1 cache entry, 1 plan.
        Token::IntLit(v) => {
            literals.push(Literal::Int(*v));
            hash_byte(h, 0xF0)
        }
        Token::FloatLit(v) => {
            literals.push(Literal::Float(*v));
            hash_byte(h, 0xF1)
        }
        Token::StringLit(s) => {
            literals.push(Literal::String(s.clone()));
            hash_byte(h, 0xF2)
        }
        Token::BoolLit(v) => {
            literals.push(Literal::Bool(*v));
            hash_byte(h, 0xF3)
        }

        // Keywords. Each gets a unique tag in the 0x10..0x4F range.
        Token::Type         => hash_byte(h, 0x10),
        Token::Filter       => hash_byte(h, 0x11),
        Token::Order        => hash_byte(h, 0x12),
        Token::Limit        => hash_byte(h, 0x13),
        Token::Offset       => hash_byte(h, 0x14),
        Token::Insert       => hash_byte(h, 0x15),
        Token::Update       => hash_byte(h, 0x16),
        Token::Delete       => hash_byte(h, 0x17),
        Token::Upsert       => hash_byte(h, 0x18),
        Token::Select       => hash_byte(h, 0x19),
        Token::Required     => hash_byte(h, 0x1A),
        Token::Multi        => hash_byte(h, 0x1B),
        Token::Link         => hash_byte(h, 0x1C),
        Token::Index        => hash_byte(h, 0x1D),
        Token::On           => hash_byte(h, 0x1E),
        Token::Asc          => hash_byte(h, 0x1F),
        Token::Desc         => hash_byte(h, 0x20),
        Token::And          => hash_byte(h, 0x21),
        Token::Or           => hash_byte(h, 0x22),
        Token::Not          => hash_byte(h, 0x23),
        Token::Exists       => hash_byte(h, 0x24),
        Token::Let          => hash_byte(h, 0x25),
        Token::As           => hash_byte(h, 0x26),
        Token::Match        => hash_byte(h, 0x27),
        Token::Group        => hash_byte(h, 0x28),
        Token::Transaction  => hash_byte(h, 0x29),
        Token::View         => hash_byte(h, 0x2A),
        Token::Materialized => hash_byte(h, 0x2B),
        Token::Count        => hash_byte(h, 0x2C),
        Token::Avg          => hash_byte(h, 0x2D),
        Token::Sum          => hash_byte(h, 0x2E),
        Token::Min          => hash_byte(h, 0x2F),
        Token::Max          => hash_byte(h, 0x30),
        Token::Join         => hash_byte(h, 0x31),
        Token::Inner        => hash_byte(h, 0x32),
        Token::LeftKw       => hash_byte(h, 0x33),
        Token::RightKw      => hash_byte(h, 0x34),
        Token::Outer        => hash_byte(h, 0x35),
        Token::Cross        => hash_byte(h, 0x36),
        Token::Distinct     => hash_byte(h, 0x37),
        Token::In           => hash_byte(h, 0x38),
        Token::Between      => hash_byte(h, 0x39),
        Token::Like         => hash_byte(h, 0x3A),
        Token::Having       => hash_byte(h, 0x3B),

        // Operators.
        Token::Eq       => hash_byte(h, 0x40),
        Token::Neq      => hash_byte(h, 0x41),
        Token::Lt       => hash_byte(h, 0x42),
        Token::Gt       => hash_byte(h, 0x43),
        Token::Lte      => hash_byte(h, 0x44),
        Token::Gte      => hash_byte(h, 0x45),
        Token::Assign   => hash_byte(h, 0x46),
        Token::Arrow    => hash_byte(h, 0x47),
        Token::Pipe     => hash_byte(h, 0x48),
        Token::Coalesce => hash_byte(h, 0x49),
        Token::Plus     => hash_byte(h, 0x4A),
        Token::Minus    => hash_byte(h, 0x4B),
        Token::Star     => hash_byte(h, 0x4C),
        Token::Slash    => hash_byte(h, 0x4D),

        // Delimiters.
        Token::LBrace => hash_byte(h, 0x60),
        Token::RBrace => hash_byte(h, 0x61),
        Token::LParen => hash_byte(h, 0x62),
        Token::RParen => hash_byte(h, 0x63),
        Token::Comma  => hash_byte(h, 0x64),
        Token::Colon  => hash_byte(h, 0x65),
        Token::Dot    => hash_byte(h, 0x66),

        Token::Eof => hash_byte(h, 0x7F),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_query_same_hash() {
        let (h1, lits1) = canonicalize("User filter .age > 30").unwrap();
        let (h2, lits2) = canonicalize("User filter .age > 30").unwrap();
        assert_eq!(h1, h2);
        assert_eq!(lits1, lits2);
        assert_eq!(lits1, vec![Literal::Int(30)]);
    }

    #[test]
    fn test_different_literal_same_hash() {
        // The whole point of the module — `30` and `99` collapse to the
        // same canonical form.
        let (h1, lits1) = canonicalize("User filter .age > 30").unwrap();
        let (h2, lits2) = canonicalize("User filter .age > 99").unwrap();
        assert_eq!(h1, h2);
        assert_eq!(lits1, vec![Literal::Int(30)]);
        assert_eq!(lits2, vec![Literal::Int(99)]);
    }

    #[test]
    fn test_whitespace_and_comments_normalised() {
        let (h1, _) = canonicalize("User filter .age > 30").unwrap();
        let (h2, _) = canonicalize("User  filter\n.age   >   30   # foo\n").unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_different_field_different_hash() {
        let (h1, _) = canonicalize("User filter .age > 30").unwrap();
        let (h2, _) = canonicalize("User filter .id > 30").unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_different_table_different_hash() {
        let (h1, _) = canonicalize("User filter .age > 30").unwrap();
        let (h2, _) = canonicalize("Order filter .age > 30").unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_different_operator_different_hash() {
        let (h1, _) = canonicalize("User filter .age > 30").unwrap();
        let (h2, _) = canonicalize("User filter .age < 30").unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_string_literal_canonicalised() {
        let (h1, lits1) = canonicalize(r#"User filter .status = "active""#).unwrap();
        let (h2, lits2) = canonicalize(r#"User filter .status = "pending""#).unwrap();
        assert_eq!(h1, h2);
        assert_eq!(lits1, vec![Literal::String("active".into())]);
        assert_eq!(lits2, vec![Literal::String("pending".into())]);
    }

    #[test]
    fn test_multi_literal_collected_in_source_order() {
        let (_, lits) = canonicalize(
            r#"User filter .age > 30 and .status = "active" { .name }"#,
        ).unwrap();
        assert_eq!(lits, vec![
            Literal::Int(30),
            Literal::String("active".into()),
        ]);
    }

    #[test]
    fn test_insert_literals_in_assignment_order() {
        let (_, lits) = canonicalize(
            r#"insert User { id := 42, name := "Alice", age := 30 }"#,
        ).unwrap();
        assert_eq!(lits, vec![
            Literal::Int(42),
            Literal::String("Alice".into()),
            Literal::Int(30),
        ]);
    }

    #[test]
    fn test_update_by_pk_literals_in_source_order() {
        let (_, lits) = canonicalize(
            "User filter .id = 42 update { age := 31 }",
        ).unwrap();
        assert_eq!(lits, vec![Literal::Int(42), Literal::Int(31)]);
    }
}
