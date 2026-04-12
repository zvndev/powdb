#![no_main]
use libfuzzer_sys::fuzz_target;
use powdb_query::parser::parse;

fuzz_target!(|data: &[u8]| {
    // Only fuzz valid UTF-8 — the parser takes &str
    if let Ok(s) = std::str::from_utf8(data) {
        // Must never panic, only return Ok/Err
        let _ = parse(s);
    }
});
