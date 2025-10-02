package communication

import (
	"encoding/binary"
	"net"
	"time"
)

const getResponseMessage = "GET_RESPONSES"

func (s *Socket) BindAndListen(address string) error {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	s.listener = ln
	return nil
}

func (s *Socket) Accept() (*Socket, error) {
	if s.listener == nil {
		return nil, net.ErrClosed
	}
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, err
	}
	return &Socket{conn: conn}, nil
}

type Socket struct {
	conn     net.Conn
	listener net.Listener
}

func (s *Socket) Connect(address string) error {
	var (
		conn net.Conn
		err  error
	)
	backoff := 100 * time.Millisecond
	maxBackoff := 3 * time.Second
	for attempts := 0; attempts < 5; attempts++ {
		conn, err = net.Dial("tcp", address)
		if err == nil {
			s.conn = conn
			return nil
		}
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return err
}

func (s *Socket) SendBatch(data []byte) error {
	if s.conn == nil {
		return net.ErrClosed
	}
	dataLen := len(data)
	binary.Write(s.conn, binary.BigEndian, uint32(dataLen))

	total_sent := 0
	for total_sent < dataLen {
		n, err := s.conn.Write(data[total_sent:])
		if err != nil {
			return err
		}
		total_sent += n
	}
	return nil
}
func (s *Socket) ReadBatch() ([]byte, error) {
	if s.conn == nil {
		return nil, net.ErrClosed
	}

	var dataLen uint32
	err := binary.Read(s.conn, binary.BigEndian, &dataLen)
	if err != nil {
		return nil, err
	}
	size := int(dataLen)
	buf := make([]byte, size)

	total := 0
	for total < size {
		n, err := s.conn.Read(buf[total:])
		if err != nil {
			return nil, err
		}
		total += n
	}
	return buf, nil
}

/*
func (s *Socket) RecvAck() (uint32, error) {
	if s.conn == nil {
		return 0, net.ErrClosed
	}
	var buf [4]byte
	total := 0
	for total < len(buf) {
		n, err := s.conn.Read(buf[total:])
		if err != nil {
			return 0, err
		}
		total += n
	}
	seq := binary.BigEndian.Uint32(buf[:])
	return seq, nil
}
*/
// Close closes the connection and listener if present.
func (s *Socket) Close() error {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Socket) SendGetResponsesRequest() error {
	if s.conn == nil {
		return net.ErrClosed
	}
	request := []byte(getResponseMessage)
	return s.SendBatch(request)
}

func IsResponseRequest(data []byte) bool {
	return string(data) == getResponseMessage
}
