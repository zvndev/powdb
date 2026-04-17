package dev.zvn.powdb;

import java.util.List;

/**
 * Wire protocol message. Sealed over the eight frame types.
 *
 * Mirrors {@code crates/server/src/protocol.rs}.
 */
public sealed interface Message {
    record Connect(String dbName, String password) implements Message {}
    record ConnectOk(String version) implements Message {}
    record Query(String query) implements Message {}
    record ResultRows(List<String> columns, List<List<String>> rows) implements Message {}
    record ResultScalar(String value) implements Message {}
    record ResultOk(long affected) implements Message {}
    record Error(String message) implements Message {}
    record Disconnect() implements Message {}
}
