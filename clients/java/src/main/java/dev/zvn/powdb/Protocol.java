package dev.zvn.powdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * PowDB wire protocol encoder/decoder.
 *
 * Frame format: {@code [type(1)][flags(1)][len(4 LE)][payload]}.
 * Strings are encoded as {@code [len(4 LE)][utf-8 bytes]}.
 */
public final class Protocol {

    public static final byte MSG_CONNECT = 0x01;
    public static final byte MSG_CONNECT_OK = 0x02;
    public static final byte MSG_QUERY = 0x03;
    public static final byte MSG_RESULT_ROWS = 0x07;
    public static final byte MSG_RESULT_SCALAR = 0x08;
    public static final byte MSG_RESULT_OK = 0x09;
    public static final byte MSG_ERROR = 0x0A;
    public static final byte MSG_DISCONNECT = 0x10;

    private Protocol() {}

    /** Carrier for decode results (the parsed message + bytes consumed). */
    public record Decoded(Message message, int consumed) {}

    public static byte[] encode(Message msg) {
        byte msgType;
        byte[] payload;
        switch (msg) {
            case Message.Connect c -> {
                msgType = MSG_CONNECT;
                byte[] dbBuf = encodeString(c.dbName());
                byte[] pwBuf = c.password() == null ? u32Le(0) : encodeString(c.password());
                payload = concat(dbBuf, pwBuf);
            }
            case Message.ConnectOk c -> {
                msgType = MSG_CONNECT_OK;
                payload = encodeString(c.version());
            }
            case Message.Query q -> {
                msgType = MSG_QUERY;
                payload = encodeString(q.query());
            }
            case Message.ResultRows r -> {
                msgType = MSG_RESULT_ROWS;
                ByteBuffer bb = ByteBuffer.allocate(estimateRowsSize(r)).order(ByteOrder.LITTLE_ENDIAN);
                bb.putShort((short) r.columns().size());
                for (String c : r.columns()) appendString(bb, c);
                bb.putInt(r.rows().size());
                for (List<String> row : r.rows()) {
                    for (String v : row) appendString(bb, v);
                }
                payload = new byte[bb.position()];
                bb.flip();
                bb.get(payload);
            }
            case Message.ResultScalar s -> {
                msgType = MSG_RESULT_SCALAR;
                payload = encodeString(s.value());
            }
            case Message.ResultOk o -> {
                msgType = MSG_RESULT_OK;
                payload = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(o.affected()).array();
            }
            case Message.Error e -> {
                msgType = MSG_ERROR;
                payload = encodeString(e.message());
            }
            case Message.Disconnect d -> {
                msgType = MSG_DISCONNECT;
                payload = new byte[0];
            }
        }
        byte[] frame = new byte[6 + payload.length];
        frame[0] = msgType;
        frame[1] = 0;
        ByteBuffer.wrap(frame, 2, 4).order(ByteOrder.LITTLE_ENDIAN).putInt(payload.length);
        System.arraycopy(payload, 0, frame, 6, payload.length);
        return frame;
    }

    /**
     * Tries to decode a single frame from the first {@code length} bytes of {@code buf}.
     * Returns {@code null} if more bytes are needed.
     *
     * @throws ProtocolException if the frame is malformed
     */
    public static Decoded tryDecode(byte[] buf, int length) {
        if (length < 6) return null;
        byte msgType = buf[0];
        int payloadLen = ByteBuffer.wrap(buf, 2, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (payloadLen < 0) throw new ProtocolException("negative payload length");
        if (length < 6 + payloadLen) return null;
        ByteBuffer payload = ByteBuffer.wrap(buf, 6, payloadLen).order(ByteOrder.LITTLE_ENDIAN);
        return new Decoded(decodePayload(msgType, payload), 6 + payloadLen);
    }

    private static Message decodePayload(byte msgType, ByteBuffer p) {
        switch (msgType) {
            case MSG_CONNECT: {
                String db = decodeString(p);
                String pw = null;
                if (p.remaining() > 0) {
                    String s = decodeString(p);
                    pw = s.isEmpty() ? null : s;
                }
                return new Message.Connect(db, pw);
            }
            case MSG_CONNECT_OK:
                return new Message.ConnectOk(decodeString(p));
            case MSG_QUERY:
                return new Message.Query(decodeString(p));
            case MSG_RESULT_ROWS: {
                if (p.remaining() < 2) throw new ProtocolException("truncated column count");
                int colCount = Short.toUnsignedInt(p.getShort());
                List<String> cols = new ArrayList<>(colCount);
                for (int i = 0; i < colCount; i++) cols.add(decodeString(p));
                if (p.remaining() < 4) throw new ProtocolException("truncated row count");
                int rowCount = p.getInt();
                if (rowCount < 0) throw new ProtocolException("negative row count");
                List<List<String>> rows = new ArrayList<>(rowCount);
                for (int r = 0; r < rowCount; r++) {
                    List<String> row = new ArrayList<>(colCount);
                    for (int c = 0; c < colCount; c++) row.add(decodeString(p));
                    rows.add(row);
                }
                return new Message.ResultRows(cols, rows);
            }
            case MSG_RESULT_SCALAR:
                return new Message.ResultScalar(decodeString(p));
            case MSG_RESULT_OK: {
                if (p.remaining() < 8) throw new ProtocolException("truncated result ok payload");
                return new Message.ResultOk(p.getLong());
            }
            case MSG_ERROR:
                return new Message.Error(decodeString(p));
            case MSG_DISCONNECT:
                return new Message.Disconnect();
            default:
                throw new ProtocolException("unknown message type: 0x" + Integer.toHexString(msgType & 0xFF));
        }
    }

    private static byte[] encodeString(String s) {
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[4 + data.length];
        ByteBuffer.wrap(out, 0, 4).order(ByteOrder.LITTLE_ENDIAN).putInt(data.length);
        System.arraycopy(data, 0, out, 4, data.length);
        return out;
    }

    private static void appendString(ByteBuffer bb, String s) {
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        bb.putInt(data.length);
        bb.put(data);
    }

    private static String decodeString(ByteBuffer p) {
        if (p.remaining() < 4) throw new ProtocolException("truncated string length");
        int len = p.getInt();
        if (len < 0 || p.remaining() < len) throw new ProtocolException("truncated string data");
        byte[] data = new byte[len];
        p.get(data);
        return new String(data, StandardCharsets.UTF_8);
    }

    private static int estimateRowsSize(Message.ResultRows r) {
        int size = 2 + 4;
        for (String c : r.columns()) size += 4 + c.getBytes(StandardCharsets.UTF_8).length;
        for (List<String> row : r.rows()) {
            for (String v : row) size += 4 + v.getBytes(StandardCharsets.UTF_8).length;
        }
        return size;
    }

    private static byte[] u32Le(int n) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(n).array();
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
