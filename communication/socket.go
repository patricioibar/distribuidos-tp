package communication

import (
	"net"
)

type Socket struct {
	conn     net.Conn
	listener net.Listener
}

func (s *Socket) Connect(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

/*
	func (s *Socket) SendChunksCount(numChunks int) error {
		if s.conn == nil {
			return net.ErrClosed
		}
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(numChunks))
		total := 0
		for total < len(buf) {
			n, err := s.conn.Write(buf[total:])
			if err != nil {
				return err
			}
			total += n
		}
		return nil
	}
*/
func (s *Socket) SendBatch(data []byte) error {
	if s.conn == nil {
		return net.ErrClosed
	}

	total_sent := 0
	dataLen := len(data)
	for total_sent < dataLen {
		n, err := s.conn.Write(data[total_sent:])
		if err != nil {
			return err
		}
		total_sent += n
	}
	return nil
}
func (s *Socket) ReadBatch(size int) ([]byte, error) {
	if s.conn == nil {
		return nil, net.ErrClosed
	}
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
