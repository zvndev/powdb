package dev.zvn.powdb;

import java.util.List;

/** Typed reply from {@link Client#query(String)}. */
public sealed interface QueryResult {
    record Rows(List<String> columns, List<List<String>> rows) implements QueryResult {}
    record Scalar(String value) implements QueryResult {}
    record Ok(long affected) implements QueryResult {}
}
