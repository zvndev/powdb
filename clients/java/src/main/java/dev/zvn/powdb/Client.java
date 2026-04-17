package dev.zvn.powdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

/**
 * Synchronous PowDB client.
 *
 * <pre>{@code
 * try (Client c = Client.connect("127.0.0.1", 5433)) {
 *     QueryResult r = c.query("User filter .age > 27 { .name, .age }");
 *     if (r instanceof QueryResult.Rows rows) {
 *         System.out.println(rows.columns());
 *         rows.rows().forEach(System.out::println);
 *     }
 * }
 * }</pre>
 *
 * <p>Not thread-safe: calls must be externally serialised.
 */
public final class Client implements AutoCloseable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private String serverVersion = "";
    private byte[] buffer = new byte[0];
    private boolean closed = false;

    private Client(Socket socket) throws IOException {
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
    }

    /** Connect with defaults: db "default", no password, 5s timeout. */
    public static Client connect(String host, int port) throws IOException {
        return connect(host, port, new Options());
    }

    public static Client connect(String host, int port, Options opts) throws IOException {
        Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port), (int) opts.connectTimeout.toMillis());

        Client c = new Client(socket);
        try {
            c.writeMessage(new Message.Connect(opts.dbName, opts.password));
            Message reply = c.readMessage();
            switch (reply) {
                case Message.ConnectOk ok -> c.serverVersion = ok.version();
                case Message.Error e -> throw new PowDBException("connect failed: " + e.message());
                default -> throw new PowDBException(
                        "expected ConnectOk, got " + reply.getClass().getSimpleName());
            }
        } catch (IOException | RuntimeException e) {
            socket.close();
            throw e;
        }
        return c;
    }

    public String serverVersion() {
        return serverVersion;
    }

    public QueryResult query(String q) throws IOException {
        if (closed) throw new PowDBException("client is closed");
        writeMessage(new Message.Query(q));
        Message reply = readMessage();
        return switch (reply) {
            case Message.ResultRows r -> new QueryResult.Rows(r.columns(), r.rows());
            case Message.ResultScalar s -> new QueryResult.Scalar(s.value());
            case Message.ResultOk o -> new QueryResult.Ok(o.affected());
            case Message.Error e -> throw new PowDBException("query failed: " + e.message());
            default -> throw new PowDBException("unexpected reply: " + reply.getClass().getSimpleName());
        };
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            writeMessage(new Message.Disconnect());
        } catch (IOException ignored) {
            // socket may already be half-closed
        }
        socket.close();
    }

    private void writeMessage(Message m) throws IOException {
        out.write(Protocol.encode(m));
        out.flush();
    }

    private Message readMessage() throws IOException {
        while (true) {
            Protocol.Decoded d = Protocol.tryDecode(buffer, buffer.length);
            if (d != null) {
                byte[] leftover = new byte[buffer.length - d.consumed()];
                System.arraycopy(buffer, d.consumed(), leftover, 0, leftover.length);
                buffer = leftover;
                return d.message();
            }
            byte[] chunk = new byte[65536];
            int n = in.read(chunk);
            if (n < 0) throw new PowDBException("connection closed by server");
            if (n > 0) {
                byte[] grown = new byte[buffer.length + n];
                System.arraycopy(buffer, 0, grown, 0, buffer.length);
                System.arraycopy(chunk, 0, grown, buffer.length, n);
                buffer = grown;
            }
        }
    }

    /** Connection options for {@link Client#connect(String, int, Options)}. */
    public static final class Options {
        public String dbName = "default";
        public String password = null;
        public Duration connectTimeout = Duration.ofSeconds(5);

        public Options dbName(String v) { this.dbName = v; return this; }
        public Options password(String v) { this.password = v; return this; }
        public Options connectTimeout(Duration v) { this.connectTimeout = v; return this; }
    }
}
