/**
 * BataDB TypeScript client.
 *
 * Thin async wrapper around a TCP socket speaking the BataDB wire protocol.
 *
 *     const client = await Client.connect({
 *       host: "213.188.194.202",
 *       port: 5433,
 *       dbName: "default",
 *       password: process.env.BATADB_PASSWORD,
 *     });
 *
 *     const result = await client.query("User filter .age > 27 { .name, .age }");
 *     await client.close();
 */

import * as net from "node:net";
import { encode, tryDecode, type Message } from "./protocol.js";

export type QueryResult =
  | { kind: "rows"; columns: string[]; rows: string[][] }
  | { kind: "scalar"; value: string }
  | { kind: "ok"; affected: bigint };

export interface ClientOptions {
  host: string;
  port: number;
  dbName?: string;
  password?: string | null;
  /** Connection timeout in ms. Defaults to 5000. */
  connectTimeoutMs?: number;
}

type Pending = {
  resolve: (msg: Message) => void;
  reject: (err: Error) => void;
};

export class Client {
  private readonly socket: net.Socket;
  private buffer: Buffer = Buffer.alloc(0);
  private readonly pending: Pending[] = [];
  private closed = false;
  private closeError: Error | null = null;

  readonly serverVersion: string;

  private constructor(socket: net.Socket, serverVersion: string) {
    this.socket = socket;
    this.serverVersion = serverVersion;

    this.socket.on("data", (chunk) => this.onData(chunk));
    this.socket.on("error", (err) => this.onClose(err));
    this.socket.on("close", () => this.onClose(null));
  }

  /** Open a connection, send Connect, wait for ConnectOk. */
  static async connect(opts: ClientOptions): Promise<Client> {
    const {
      host,
      port,
      dbName = "default",
      password = null,
      connectTimeoutMs = 5000,
    } = opts;

    const socket = await openSocket(host, port, connectTimeoutMs);

    // We need to read the initial ConnectOk before wiring up the normal
    // pending-queue machinery, so we do a one-shot handshake here.
    const handshake = new Promise<Message>((resolve, reject) => {
      let scratch = Buffer.alloc(0);
      const onData = (chunk: Buffer) => {
        scratch = Buffer.concat([scratch, chunk]);
        const decoded = tryDecode(scratch);
        if (decoded !== null) {
          socket.removeListener("data", onData);
          socket.removeListener("error", onError);
          // Any bytes past the handshake frame belong to later responses.
          // This should not happen in practice, but handle it defensively.
          const leftover = scratch.subarray(decoded.consumed);
          if (leftover.length > 0) {
            socket.unshift(leftover);
          }
          resolve(decoded.msg);
        }
      };
      const onError = (err: Error) => {
        socket.removeListener("data", onData);
        reject(err);
      };
      socket.on("data", onData);
      socket.on("error", onError);
    });

    socket.write(encode({ type: "Connect", dbName, password }));
    const reply = await handshake;

    if (reply.type === "Error") {
      socket.destroy();
      throw new Error(`connect failed: ${reply.message}`);
    }
    if (reply.type !== "ConnectOk") {
      socket.destroy();
      throw new Error(`expected ConnectOk, got ${reply.type}`);
    }

    return new Client(socket, reply.version);
  }

  /** Run a BataQL statement and return the typed result. */
  async query(query: string): Promise<QueryResult> {
    const reply = await this.send({ type: "Query", query });
    switch (reply.type) {
      case "ResultRows":
        return { kind: "rows", columns: reply.columns, rows: reply.rows };
      case "ResultScalar":
        return { kind: "scalar", value: reply.value };
      case "ResultOk":
        return { kind: "ok", affected: reply.affected };
      case "Error":
        throw new Error(`query failed: ${reply.message}`);
      default:
        throw new Error(`unexpected reply: ${reply.type}`);
    }
  }

  /** Send Disconnect and tear down the socket. */
  async close(): Promise<void> {
    if (this.closed) return;
    try {
      this.socket.write(encode({ type: "Disconnect" }));
    } catch {
      // socket may already be half-closed; ignore
    }
    this.closed = true;
    await new Promise<void>((resolve) => {
      this.socket.end(() => resolve());
    });
  }

  // ───── internals ─────────────────────────────────────────────────────────

  private send(msg: Message): Promise<Message> {
    if (this.closed) {
      return Promise.reject(
        this.closeError ?? new Error("client is closed"),
      );
    }
    return new Promise((resolve, reject) => {
      this.pending.push({ resolve, reject });
      this.socket.write(encode(msg), (err) => {
        if (err) {
          // Writer error — the promise will also be rejected by onClose,
          // but rejecting here gives a faster, more specific failure.
          const entry = this.pending.shift();
          if (entry) entry.reject(err);
        }
      });
    });
  }

  private onData(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (true) {
      const decoded = tryDecode(this.buffer);
      if (decoded === null) break;
      this.buffer = this.buffer.subarray(decoded.consumed);
      const entry = this.pending.shift();
      if (!entry) {
        // Server sent an unsolicited frame. Treat as protocol error.
        this.onClose(new Error("received unexpected frame from server"));
        return;
      }
      entry.resolve(decoded.msg);
    }
  }

  private onClose(err: Error | null): void {
    if (this.closed && err === null) return;
    this.closed = true;
    this.closeError = err;
    const error = err ?? new Error("connection closed");
    while (this.pending.length > 0) {
      const entry = this.pending.shift()!;
      entry.reject(error);
    }
  }
}

function openSocket(
  host: string,
  port: number,
  timeoutMs: number,
): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error(`connect timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    socket.once("connect", () => {
      clearTimeout(timer);
      socket.setNoDelay(true);
      resolve(socket);
    });
    socket.once("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
    socket.connect(port, host);
  });
}

export { encode, tryDecode } from "./protocol.js";
export type { Message } from "./protocol.js";
