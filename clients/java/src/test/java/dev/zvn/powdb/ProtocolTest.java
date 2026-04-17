package dev.zvn.powdb;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolTest {

    private Message roundtrip(Message in) {
        byte[] bytes = Protocol.encode(in);
        Protocol.Decoded d = Protocol.tryDecode(bytes, bytes.length);
        assertNotNull(d);
        assertEquals(bytes.length, d.consumed());
        return d.message();
    }

    @Test
    void connectWithPassword() {
        Message out = roundtrip(new Message.Connect("default", "secret"));
        Message.Connect c = (Message.Connect) out;
        assertEquals("default", c.dbName());
        assertEquals("secret", c.password());
    }

    @Test
    void connectNoPassword() {
        Message out = roundtrip(new Message.Connect("default", null));
        assertNull(((Message.Connect) out).password());
    }

    @Test
    void connectOk() {
        Message out = roundtrip(new Message.ConnectOk("0.1.2"));
        assertEquals("0.1.2", ((Message.ConnectOk) out).version());
    }

    @Test
    void query() {
        Message out = roundtrip(new Message.Query("User filter .age > 30"));
        assertEquals("User filter .age > 30", ((Message.Query) out).query());
    }

    @Test
    void resultRows() {
        List<List<String>> rows = Arrays.asList(
                Arrays.asList("Alice", "30"),
                Arrays.asList("Bob", "25"));
        Message out = roundtrip(new Message.ResultRows(Arrays.asList("name", "age"), rows));
        Message.ResultRows r = (Message.ResultRows) out;
        assertEquals(Arrays.asList("name", "age"), r.columns());
        assertEquals(rows, r.rows());
    }

    @Test
    void resultRowsEmpty() {
        Message out = roundtrip(new Message.ResultRows(
                Collections.singletonList("x"), Collections.emptyList()));
        assertTrue(((Message.ResultRows) out).rows().isEmpty());
    }

    @Test
    void resultScalar() {
        Message out = roundtrip(new Message.ResultScalar("42"));
        assertEquals("42", ((Message.ResultScalar) out).value());
    }

    @Test
    void resultOk() {
        Message out = roundtrip(new Message.ResultOk(1L << 40));
        assertEquals(1L << 40, ((Message.ResultOk) out).affected());
    }

    @Test
    void errorMessage() {
        Message out = roundtrip(new Message.Error("table not found"));
        assertEquals("table not found", ((Message.Error) out).message());
    }

    @Test
    void disconnect() {
        Message out = roundtrip(new Message.Disconnect());
        assertTrue(out instanceof Message.Disconnect);
    }

    @Test
    void partialFrameReturnsNull() {
        byte[] full = Protocol.encode(new Message.Query("hello"));
        assertNull(Protocol.tryDecode(full, 3));
        assertNull(Protocol.tryDecode(full, full.length - 1));
    }

    @Test
    void utf8Roundtrip() {
        Message out = roundtrip(new Message.ResultScalar("café ☕ 日本語"));
        assertEquals("café ☕ 日本語", ((Message.ResultScalar) out).value());
    }

    @Test
    void unknownTypeThrows() {
        byte[] frame = Protocol.encode(new Message.Disconnect());
        frame[0] = (byte) 0xFF;
        assertThrows(ProtocolException.class, () -> Protocol.tryDecode(frame, frame.length));
    }
}
