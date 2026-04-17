package powdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// QueryResult is the union of possible query replies: *Rows, *Scalar, or *Ok.
type QueryResult interface{ isQueryResult() }

// Rows is a tabular result.
type Rows struct {
	Columns []string
	Rows    [][]string
}

// Scalar is a single-value result.
type Scalar struct{ Value string }

// Ok is a write acknowledgement.
type Ok struct{ Affected uint64 }

func (*Rows) isQueryResult()   {}
func (*Scalar) isQueryResult() {}
func (*Ok) isQueryResult()     {}

// Options configures a new Client.
type Options struct {
	Host           string
	Port           int
	DBName         string        // defaults to "default"
	Password       *string       // nil means unauthenticated
	ConnectTimeout time.Duration // defaults to 5s
}

// Client is a synchronous PowDB connection.
//
// A Client is safe for serial use; concurrent calls to Query must be externally
// serialised (e.g. via a pool or mutex). The wire protocol has no request IDs,
// so replies must be consumed in send order.
type Client struct {
	conn          net.Conn
	buf           []byte
	mu            sync.Mutex
	closed        bool
	ServerVersion string
}

// Dial opens a connection to the server, performs the handshake, and returns a ready client.
func Dial(ctx context.Context, opts Options) (*Client, error) {
	if opts.DBName == "" {
		opts.DBName = "default"
	}
	if opts.ConnectTimeout == 0 {
		opts.ConnectTimeout = 5 * time.Second
	}
	d := net.Dialer{Timeout: opts.ConnectTimeout}
	addr := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	c := &Client{conn: conn}
	if _, err := conn.Write(Encode(Connect{DBName: opts.DBName, Password: opts.Password})); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send connect: %w", err)
	}
	reply, err := c.readOne()
	if err != nil {
		conn.Close()
		return nil, err
	}
	switch r := reply.(type) {
	case ConnectOk:
		c.ServerVersion = r.Version
		return c, nil
	case Error:
		conn.Close()
		return nil, fmt.Errorf("connect failed: %s", r.Message)
	default:
		conn.Close()
		return nil, fmt.Errorf("expected ConnectOk, got %T", reply)
	}
}

// Query runs a PowQL statement and returns the typed result.
func (c *Client) Query(query string) (QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("client is closed")
	}
	if _, err := c.conn.Write(Encode(Query{Query: query})); err != nil {
		return nil, err
	}
	reply, err := c.readOne()
	if err != nil {
		return nil, err
	}
	switch r := reply.(type) {
	case ResultRows:
		return &Rows{Columns: r.Columns, Rows: r.Rows}, nil
	case ResultScalar:
		return &Scalar{Value: r.Value}, nil
	case ResultOk:
		return &Ok{Affected: r.Affected}, nil
	case Error:
		return nil, fmt.Errorf("query failed: %s", r.Message)
	default:
		return nil, fmt.Errorf("unexpected reply: %T", reply)
	}
}

// Close sends Disconnect and tears down the TCP connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	_, _ = c.conn.Write(Encode(Disconnect{}))
	return c.conn.Close()
}

func (c *Client) readOne() (Message, error) {
	for {
		if msg, consumed, err := TryDecode(c.buf); err != nil {
			return nil, err
		} else if msg != nil {
			c.buf = c.buf[consumed:]
			return msg, nil
		}
		chunk := make([]byte, 64*1024)
		n, err := c.conn.Read(chunk)
		if n > 0 {
			c.buf = append(c.buf, chunk[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				return nil, errors.New("connection closed by server")
			}
			return nil, err
		}
	}
}
