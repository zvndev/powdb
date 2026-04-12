#![no_main]
use libfuzzer_sys::fuzz_target;
use powdb_query::lexer::lex;
use powdb_query::parser::parse;

fuzz_target!(|data: &[u8]| {
    // Only fuzz valid UTF-8
    if let Ok(s) = std::str::from_utf8(data) {
        // Phase 1: lex → parse the raw input
        let tokens = match lex(s) {
            Ok(t) => t,
            Err(_) => return,
        };
        let _stmt = match parse(s) {
            Ok(s) => s,
            Err(_) => return,
        };

        // Phase 2: if we successfully parsed, re-lex the same input and verify
        // token streams are deterministic (same input → same tokens)
        let tokens2 = lex(s).expect("re-lex of parseable input must not fail");
        assert_eq!(
            tokens.len(),
            tokens2.len(),
            "lexer is non-deterministic on: {s:?}"
        );
    }
});
