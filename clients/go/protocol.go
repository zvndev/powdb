// Package powdb implements a Go client for PowDB.
//
// Frame format on the wire: [type(1)][flags(1)][len(4 LE)][payload].
// Strings are encoded as [len(4 LE)][utf-8 bytes].
//
// Mirrors crates/server/src/protocol.rs.
package powdb

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	msgConnect      byte = 0x01
	msgConnectOk    byte = 0x02
	msgQuery        byte = 0x03
	msgResultRows   byte = 0x07
	msgResultScalar byte = 0x08
	msgResultOk     byte = 0x09
	msgError        byte = 0x0A
	msgDisconnect   byte = 0x10
)

// Message is the union of all wire frames.
type Message interface{ isMessage() }

// Connect opens a session.
type Connect struct {
	DBName   string
	Password *string
}

// ConnectOk is the server's handshake reply.
type ConnectOk struct{ Version string }

// Query carries a PowQL statement.
type Query struct{ Query string }

// ResultRows is a tabular result.
type ResultRows struct {
	Columns []string
	Rows    [][]string
}

// ResultScalar is a single-value result.
type ResultScalar struct{ Value string }

// ResultOk indicates a write with the number of affected rows.
type ResultOk struct{ Affected uint64 }

// Error is a server-side error reply.
type Error struct{ Message string }

// Disconnect ends the session.
type Disconnect struct{}

func (Connect) isMessage()      {}
func (ConnectOk) isMessage()    {}
func (Query) isMessage()        {}
func (ResultRows) isMessage()   {}
func (ResultScalar) isMessage() {}
func (ResultOk) isMessage()     {}
func (Error) isMessage()        {}
func (Disconnect) isMessage()   {}

// Encode serialises a message into a full frame.
func Encode(m Message) []byte {
	var msgType byte
	var payload []byte

	switch v := m.(type) {
	case Connect:
		payload = encodeString(v.DBName)
		if v.Password == nil {
			payload = append(payload, 0, 0, 0, 0)
		} else {
			payload = append(payload, encodeString(*v.Password)...)
		}
		msgType = msgConnect
	case ConnectOk:
		payload = encodeString(v.Version)
		msgType = msgConnectOk
	case Query:
		payload = encodeString(v.Query)
		msgType = msgQuery
	case ResultRows:
		payload = make([]byte, 2)
		binary.LittleEndian.PutUint16(payload, uint16(len(v.Columns)))
		for _, c := range v.Columns {
			payload = append(payload, encodeString(c)...)
		}
		rc := make([]byte, 4)
		binary.LittleEndian.PutUint32(rc, uint32(len(v.Rows)))
		payload = append(payload, rc...)
		for _, row := range v.Rows {
			for _, val := range row {
				payload = append(payload, encodeString(val)...)
			}
		}
		msgType = msgResultRows
	case ResultScalar:
		payload = encodeString(v.Value)
		msgType = msgResultScalar
	case ResultOk:
		payload = make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, v.Affected)
		msgType = msgResultOk
	case Error:
		payload = encodeString(v.Message)
		msgType = msgError
	case Disconnect:
		msgType = msgDisconnect
	default:
		panic(fmt.Sprintf("powdb: unknown message type %T", v))
	}

	frame := make([]byte, 6+len(payload))
	frame[0] = msgType
	frame[1] = 0 // flags
	binary.LittleEndian.PutUint32(frame[2:6], uint32(len(payload)))
	copy(frame[6:], payload)
	return frame
}

// TryDecode parses a single frame from the front of buf.
//
// If buf does not yet contain a complete frame, it returns (nil, 0, nil).
// On success it returns the message and the number of bytes consumed.
func TryDecode(buf []byte) (Message, int, error) {
	if len(buf) < 6 {
		return nil, 0, nil
	}
	msgType := buf[0]
	payloadLen := int(binary.LittleEndian.Uint32(buf[2:6]))
	if len(buf) < 6+payloadLen {
		return nil, 0, nil
	}
	payload := buf[6 : 6+payloadLen]
	msg, err := decodePayload(msgType, payload)
	if err != nil {
		return nil, 0, err
	}
	return msg, 6 + payloadLen, nil
}

func decodePayload(msgType byte, payload []byte) (Message, error) {
	switch msgType {
	case msgConnect:
		pos := 0
		db, err := decodeString(payload, &pos)
		if err != nil {
			return nil, err
		}
		var pw *string
		if pos < len(payload) {
			p, err := decodeString(payload, &pos)
			if err != nil {
				return nil, err
			}
			if p != "" {
				pw = &p
			}
		}
		return Connect{DBName: db, Password: pw}, nil
	case msgConnectOk:
		pos := 0
		v, err := decodeString(payload, &pos)
		if err != nil {
			return nil, err
		}
		return ConnectOk{Version: v}, nil
	case msgQuery:
		pos := 0
		q, err := decodeString(payload, &pos)
		if err != nil {
			return nil, err
		}
		return Query{Query: q}, nil
	case msgResultRows:
		if len(payload) < 2 {
			return nil, errors.New("truncated column count")
		}
		pos := 0
		colCount := int(binary.LittleEndian.Uint16(payload[pos:]))
		pos += 2
		cols := make([]string, colCount)
		for i := 0; i < colCount; i++ {
			s, err := decodeString(payload, &pos)
			if err != nil {
				return nil, err
			}
			cols[i] = s
		}
		if pos+4 > len(payload) {
			return nil, errors.New("truncated row count")
		}
		rowCount := int(binary.LittleEndian.Uint32(payload[pos:]))
		pos += 4
		rows := make([][]string, rowCount)
		for r := 0; r < rowCount; r++ {
			row := make([]string, colCount)
			for c := 0; c < colCount; c++ {
				s, err := decodeString(payload, &pos)
				if err != nil {
					return nil, err
				}
				row[c] = s
			}
			rows[r] = row
		}
		return ResultRows{Columns: cols, Rows: rows}, nil
	case msgResultScalar:
		pos := 0
		v, err := decodeString(payload, &pos)
		if err != nil {
			return nil, err
		}
		return ResultScalar{Value: v}, nil
	case msgResultOk:
		if len(payload) < 8 {
			return nil, errors.New("truncated result ok payload")
		}
		return ResultOk{Affected: binary.LittleEndian.Uint64(payload[:8])}, nil
	case msgError:
		pos := 0
		m, err := decodeString(payload, &pos)
		if err != nil {
			return nil, err
		}
		return Error{Message: m}, nil
	case msgDisconnect:
		return Disconnect{}, nil
	default:
		return nil, fmt.Errorf("unknown message type: 0x%x", msgType)
	}
}

func encodeString(s string) []byte {
	out := make([]byte, 4+len(s))
	binary.LittleEndian.PutUint32(out, uint32(len(s)))
	copy(out[4:], s)
	return out
}

func decodeString(buf []byte, pos *int) (string, error) {
	if *pos+4 > len(buf) {
		return "", errors.New("truncated string length")
	}
	l := int(binary.LittleEndian.Uint32(buf[*pos:]))
	*pos += 4
	if *pos+l > len(buf) {
		return "", errors.New("truncated string data")
	}
	s := string(buf[*pos : *pos+l])
	*pos += l
	return s, nil
}
