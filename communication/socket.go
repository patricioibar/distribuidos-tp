package reader

import (
	"encoding/binary"
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

func (s *Socket) SendChunk(seq int, chunkLen int, data []byte) error {
	if s.conn == nil {
		return net.ErrClosed
	}

	buf := make([]byte, 8+chunkLen)

	binary.BigEndian.PutUint32(buf[0:4], uint32(seq))
	binary.BigEndian.PutUint32(buf[4:8], uint32(chunkLen))
	copy(buf[8:], data)

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

func (s *Socket) RecvAck() (int, error) {
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
	return int(seq), nil
}

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
