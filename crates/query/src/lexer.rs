use crate::token::Token;

#[derive(Debug)]
pub struct LexError {
    pub message: String,
    pub position: usize,
}

pub fn lex(input: &str) -> Result<Vec<Token>, LexError> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut pos = 0;

    while pos < chars.len() {
        // Skip whitespace
        if chars[pos].is_whitespace() {
            pos += 1;
            continue;
        }

        // Skip comments
        if chars[pos] == '#' {
            while pos < chars.len() && chars[pos] != '\n' {
                pos += 1;
            }
            continue;
        }

        // Dot-ident: .fieldname
        if chars[pos] == '.' && pos + 1 < chars.len() && (chars[pos + 1].is_alphabetic() || chars[pos + 1] == '_') {
            pos += 1; // skip dot
            let start = pos;
            while pos < chars.len() && (chars[pos].is_alphanumeric() || chars[pos] == '_') {
                pos += 1;
            }
            let name: String = chars[start..pos].iter().collect();
            tokens.push(Token::DotIdent(name));
            continue;
        }

        // Param: $name
        if chars[pos] == '$' {
            pos += 1;
            let start = pos;
            while pos < chars.len() && (chars[pos].is_alphanumeric() || chars[pos] == '_') {
                pos += 1;
            }
            let name: String = chars[start..pos].iter().collect();
            tokens.push(Token::Param(name));
            continue;
        }

        // String literal
        if chars[pos] == '"' {
            pos += 1;
            let mut s = String::new();
            while pos < chars.len() && chars[pos] != '"' {
                if chars[pos] == '\\' && pos + 1 < chars.len() {
                    match chars[pos + 1] {
                        '"' => { s.push('"'); pos += 2; }
                        '\\' => { s.push('\\'); pos += 2; }
                        'n' => { s.push('\n'); pos += 2; }
                        't' => { s.push('\t'); pos += 2; }
                        _ => { s.push(chars[pos + 1]); pos += 2; }
                    }
                } else {
                    s.push(chars[pos]);
                    pos += 1;
                }
            }
            if pos >= chars.len() {
                return Err(LexError { message: "unterminated string".into(), position: pos });
            }
            pos += 1; // closing quote
            tokens.push(Token::StringLit(s));
            continue;
        }

        // Number (int or float)
        if chars[pos].is_ascii_digit() || (chars[pos] == '-' && pos + 1 < chars.len() && chars[pos + 1].is_ascii_digit()) {
            let start = pos;
            if chars[pos] == '-' { pos += 1; }
            while pos < chars.len() && chars[pos].is_ascii_digit() { pos += 1; }
            if pos < chars.len() && chars[pos] == '.' && pos + 1 < chars.len() && chars[pos + 1].is_ascii_digit() {
                pos += 1;
                while pos < chars.len() && chars[pos].is_ascii_digit() { pos += 1; }
                let s: String = chars[start..pos].iter().collect();
                tokens.push(Token::FloatLit(s.parse().unwrap()));
            } else {
                let s: String = chars[start..pos].iter().collect();
                tokens.push(Token::IntLit(s.parse().unwrap()));
            }
            continue;
        }

        // Identifiers and keywords
        if chars[pos].is_alphabetic() || chars[pos] == '_' {
            let start = pos;
            while pos < chars.len() && (chars[pos].is_alphanumeric() || chars[pos] == '_') {
                pos += 1;
            }
            let word: String = chars[start..pos].iter().collect();
            let token = match word.as_str() {
                "type"         => Token::Type,
                "filter"       => Token::Filter,
                "order"        => Token::Order,
                "limit"        => Token::Limit,
                "offset"       => Token::Offset,
                "insert"       => Token::Insert,
                "update"       => Token::Update,
                "delete"       => Token::Delete,
                "upsert"       => Token::Upsert,
                "select"       => Token::Select,
                "required"     => Token::Required,
                "multi"        => Token::Multi,
                "link"         => Token::Link,
                "index"        => Token::Index,
                "on"           => Token::On,
                "asc"          => Token::Asc,
                "desc"         => Token::Desc,
                "and"          => Token::And,
                "or"           => Token::Or,
                "not"          => Token::Not,
                "exists"       => Token::Exists,
                "let"          => Token::Let,
                "as"           => Token::As,
                "match"        => Token::Match,
                "group"        => Token::Group,
                "join"         => Token::Join,
                "inner"        => Token::Inner,
                "left"         => Token::LeftKw,
                "right"        => Token::RightKw,
                "outer"        => Token::Outer,
                "cross"        => Token::Cross,
                "transaction"  => Token::Transaction,
                "view"         => Token::View,
                "materialized" => Token::Materialized,
                "materialize"  => Token::Materialized,
                "refresh"      => Token::Refresh,
                "having"       => Token::Having,
                "distinct"     => Token::Distinct,
                "in"           => Token::In,
                "between"      => Token::Between,
                "like"         => Token::Like,
                "count"        => Token::Count,
                "avg"          => Token::Avg,
                "sum"          => Token::Sum,
                "min"          => Token::Min,
                "max"          => Token::Max,
                "is"           => Token::Is,
                "null"         => Token::Null,
                "upper"        => Token::Upper,
                "lower"        => Token::Lower,
                "length"       => Token::Length,
                "trim"         => Token::Trim,
                "substring"    => Token::Substring,
                "concat"       => Token::Concat,
                "case"         => Token::Case,
                "when"         => Token::When,
                "then"         => Token::Then,
                "else"         => Token::Else,
                "end"          => Token::End,
                "alter"        => Token::Alter,
                "drop"         => Token::Drop,
                "add"          => Token::Add,
                "column"       => Token::Column,
                "true"         => Token::BoolLit(true),
                "false"        => Token::BoolLit(false),
                _              => Token::Ident(word),
            };
            tokens.push(token);
            continue;
        }

        // Two-char operators
        if pos + 1 < chars.len() {
            let two: String = chars[pos..pos + 2].iter().collect();
            match two.as_str() {
                ":=" => { tokens.push(Token::Assign); pos += 2; continue; }
                "->" => { tokens.push(Token::Arrow); pos += 2; continue; }
                "!=" => { tokens.push(Token::Neq); pos += 2; continue; }
                "<=" => { tokens.push(Token::Lte); pos += 2; continue; }
                ">=" => { tokens.push(Token::Gte); pos += 2; continue; }
                "??" => { tokens.push(Token::Coalesce); pos += 2; continue; }
                _ => {}
            }
        }

        // Single-char operators
        let token = match chars[pos] {
            '=' => Token::Eq,
            '<' => Token::Lt,
            '>' => Token::Gt,
            '|' => Token::Pipe,
            '+' => Token::Plus,
            '-' => Token::Minus,
            '*' => Token::Star,
            '/' => Token::Slash,
            '{' => Token::LBrace,
            '}' => Token::RBrace,
            '(' => Token::LParen,
            ')' => Token::RParen,
            ',' => Token::Comma,
            ':' => Token::Colon,
            '.' => Token::Dot,
            c => return Err(LexError { message: format!("unexpected character: {c}"), position: pos }),
        };
        tokens.push(token);
        pos += 1;
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::Token;

    #[test]
    fn test_lex_simple_query() {
        let tokens = lex("User filter .age > 30").unwrap();
        assert_eq!(tokens, vec![
            Token::Ident("User".into()),
            Token::Filter,
            Token::DotIdent("age".into()),
            Token::Gt,
            Token::IntLit(30),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_lex_projection() {
        let tokens = lex("User { name, email }").unwrap();
        assert_eq!(tokens, vec![
            Token::Ident("User".into()),
            Token::LBrace,
            Token::Ident("name".into()),
            Token::Comma,
            Token::Ident("email".into()),
            Token::RBrace,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_lex_insert() {
        let tokens = lex(r#"insert User { name := "Alice", age := 30 }"#).unwrap();
        assert_eq!(tokens, vec![
            Token::Insert,
            Token::Ident("User".into()),
            Token::LBrace,
            Token::Ident("name".into()),
            Token::Assign,
            Token::StringLit("Alice".into()),
            Token::Comma,
            Token::Ident("age".into()),
            Token::Assign,
            Token::IntLit(30),
            Token::RBrace,
            Token::Eof,
        ]);
    }

    #[test]
    fn test_lex_params() {
        let tokens = lex("User filter .age > $min_age").unwrap();
        assert_eq!(tokens, vec![
            Token::Ident("User".into()),
            Token::Filter,
            Token::DotIdent("age".into()),
            Token::Gt,
            Token::Param("min_age".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_lex_string_with_escapes() {
        let tokens = lex(r#""hello \"world\"""#).unwrap();
        assert_eq!(tokens, vec![
            Token::StringLit("hello \"world\"".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_lex_aggregation() {
        let tokens = lex("count(User)").unwrap();
        assert_eq!(tokens, vec![
            Token::Count,
            Token::LParen,
            Token::Ident("User".into()),
            Token::RParen,
            Token::Eof,
        ]);
    }
}
