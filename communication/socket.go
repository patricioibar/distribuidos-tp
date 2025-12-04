package communication

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	uuid "github.com/google/uuid"
)

const getResponseMessage = "GET_RESPONSES"

var MonitorsCount int

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
	for attempts := 0; attempts < 10; attempts++ {
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

func (s *Socket) SendStartJobRequest() error {
	if s.conn == nil {
		return net.ErrClosed
	}
	id := uuid.Nil
	data, _ := id.MarshalBinary()
	return s.SendBatch(data)
}

func (s *Socket) SendUUID(uuid uuid.UUID) error {
	if s.conn == nil {
		return net.ErrClosed
	}
	data, err := uuid.MarshalBinary()
	if err != nil {
		return err
	}
	err = s.SendBatch(data)
	return err
}

func (s *Socket) ReceiveUUID() (uuid.UUID, error) {
	if s.conn == nil {
		return uuid.UUID{}, net.ErrClosed
	}
	data, err := s.ReadBatch()
	if err != nil {
		return uuid.UUID{}, err
	}
	var id uuid.UUID
	err = id.UnmarshalBinary(data)
	if err != nil {
		return uuid.UUID{}, err
	}
	return id, nil
}

func ResolveAddresses(nodeId string, monitorsCount int, addressList ...int) ([]*net.UDPAddr, error) {
	var monitorAddresses []*net.UDPAddr
	if len(addressList) > 0 {
		for _, monitorIdInt := range addressList {
			monitorId := fmt.Sprintf("monitor-%d", monitorIdInt)
			addrStr := fmt.Sprintf("%s:9000", monitorId)
			addr, _ := net.ResolveUDPAddr("udp", addrStr)
			monitorAddresses = append(monitorAddresses, addr)
		}
		fmt.Printf("%v\n", monitorAddresses)
	} else {
		for i := 1; i <= monitorsCount; i++ {
			monitorId := fmt.Sprintf("monitor-%d", i)
			if monitorId == nodeId {
				continue
			}
			addrStr := fmt.Sprintf("%s:9000", monitorId)
			addr, _ := net.ResolveUDPAddr("udp", addrStr)
			monitorAddresses = append(monitorAddresses, addr)
		}
	}

	return monitorAddresses, nil
}

func SendMessageToMonitors(addresses []*net.UDPAddr, msg string) {
	for _, addr := range addresses {
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			fmt.Printf("error dialing UDP to address: %v %s with message: %s \n", err, addr.String(), msg)
			continue
		}
		msg := []byte(msg)
		_, err = conn.Write(msg)
		if err != nil {
			fmt.Println("error sending message:", err)
		}
		conn.Close()
	}
}

func (s *Socket) GetRemoteAddress() string {
	return s.conn.RemoteAddr().String()
}

func SendHeartbeatToMonitors(heartBeat string, nodeID string, monitorsCount int) {
	t := time.NewTicker(250 * time.Millisecond)
	addresses, _ := ResolveAddresses(nodeID, monitorsCount)

	for range t.C {
		SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", heartBeat, nodeID))
	}
}
