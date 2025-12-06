package main

import (
	"bytes"
	"communication"
	"context"
	"encoding/gob"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type Config struct {
	Port          int
	MonitorId     string
	MonitorIdInt  int
	monitorsCount int
}

func getConfig() Config {
	monitorId := os.Getenv("MONITOR_ID")
	monitorIdInt, _ := strconv.Atoi(strings.Split(monitorId, "-")[1])
	port, _ := strconv.Atoi(os.Getenv("PORT"))
	monitorsCount, _ := strconv.Atoi(os.Getenv("MONITORS_COUNT"))

	return Config{
		Port:          port,
		MonitorId:     monitorId,
		MonitorIdInt:  monitorIdInt,
		monitorsCount: monitorsCount,
	}
}

type Monitor struct {
	mu                    sync.Mutex
	monitorLastSeen       map[string]time.Time
	workerLastSeen        map[string]time.Time
	currentLeader         string
	electionInProgress    bool
	okElectionMessageChan chan bool
	newCoordinatorChan    chan bool
}

const (
	MSG_MONITOR      byte = 1
	MSG_WORKER       byte = 2
	MSG_ELECTION     byte = 3
	MSG_OK           byte = 4
	MSG_COORDINATOR  byte = 5
	MSG_ASK_METADATA byte = 6
	MSG_METADATA     byte = 7
)

const (
	TIMEOUT = 1 * time.Second
)

func main() {
	config := getConfig()
	log.Printf("Starting...")

	monitor := &Monitor{
		mu:                    sync.Mutex{},
		monitorLastSeen:       make(map[string]time.Time),
		workerLastSeen:        make(map[string]time.Time),
		okElectionMessageChan: make(chan bool, 10),
		newCoordinatorChan:    make(chan bool, 10),
		currentLeader:         "",
		electionInProgress:    false,
	}

	addr := net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: config.Port,
	}

	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Fatalf("UDP listen error: %v", err)
	}
	defer conn.Close()

	go listenUDP(conn, config, monitor)
	go communication.SendHeartbeatToMonitors("MONITOR", config.MonitorId, config.monitorsCount)

	// espero un poco para la primera eleccion
	askForMetadata(config)
	time.Sleep(1000 * time.Millisecond)
	startElection(config, monitor)

	ticker := time.NewTicker(500 * time.Millisecond)

	for range ticker.C {
		monitor.mu.Lock()

		// resolver fallas de monitores
		for monitorID, last := range monitor.monitorLastSeen {
			if monitorID == config.MonitorId {
				continue
			}

			if time.Since(last) > TIMEOUT {
				log.Printf("Monitor %s is DOWN", monitorID)

				// si el monitor caido es el lider → inicio eleccion
				if monitorID == monitor.currentLeader {
					log.Printf("Leader %s failed → starting election", monitorID)
					monitor.currentLeader = ""

					if !monitor.electionInProgress {
						monitor.mu.Unlock()
						startElection(config, monitor)
						if config.MonitorId == monitor.currentLeader {
							log.Printf("[Leader] Restarting monitor %s...", monitorID)
							go restartWorker(monitorID)
						} else {
							log.Printf("Not the leader (%s), can't restart monitor %s", config.MonitorId, monitorID)
						}
						monitor.mu.Lock()
					}
				} else if config.MonitorId == monitor.currentLeader {
					// solo el lider puede reiniciar
					log.Printf("[Leader] Restarting monitor %s...", monitorID)
					monitor.monitorLastSeen[monitorID] = time.Now().Add(TIMEOUT) // evitar reinicios multiples
					go restartWorker(monitorID)
				}
			}
		}

		// resolver fallas de workers
		if config.MonitorId == monitor.currentLeader {
			for workerID, last := range monitor.workerLastSeen {
				if time.Since(last) > TIMEOUT {
					log.Printf("[Leader] Worker %s DOWN → restarting...", workerID)
					monitor.workerLastSeen[workerID] = time.Now().Add(TIMEOUT) // evitar reinicios multiples
					go restartWorker(workerID)
				}
			}
		}

		monitor.mu.Unlock()
	}
}

func askForMetadata(config Config) {
	addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount)
	msg := append([]byte{MSG_ASK_METADATA}, []byte(config.MonitorId)...)
	communication.SendMessageToMonitors(addresses, msg)
}

// ---------------- Message Handler ---------------- //
func listenUDP(conn *net.UDPConn, config Config, monitor *Monitor) {
	buf := make([]byte, 1024)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		handleMessage(buf[:n], config, monitor)
	}
}

func handleMessage(msg []byte, config Config, monitor *Monitor) {
	if len(msg) < 2 {
		return
	}

	msgType := msg[0]
	payload := string(msg[1:])

	switch msgType {

	case MSG_MONITOR:
		monitor.mu.Lock()
		monitor.monitorLastSeen[payload] = time.Now()
		monitor.mu.Unlock()

	case MSG_WORKER:
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		monitor.workerLastSeen[payload] = time.Now()

	case MSG_ASK_METADATA:
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		if monitor.currentLeader == config.MonitorId {
			go notifyActualLeaderAndMetadata(payload, config, monitor)
		}

	case MSG_ELECTION:
		log.Printf("Received ELECTION from %s", payload)
		senderID, _ := strconv.Atoi(strings.Split(payload, "-")[1])
		monitor.mu.Lock()
		monitor.currentLeader = ""
		monitor.mu.Unlock()

		if senderID < config.MonitorIdInt {
			log.Printf("Sending OK to %s (I have higher ID)", payload)
			addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount, senderID)
			msg := append([]byte{MSG_OK}, []byte(config.MonitorId)...)
			communication.SendMessageToMonitors(addresses, msg)
			go startElection(config, monitor)
		}

	case MSG_OK:
		select {
		case monitor.okElectionMessageChan <- true:
		default:
		}

	case MSG_COORDINATOR:
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		monitor.currentLeader = payload
		monitor.electionInProgress = false
		monitor.newCoordinatorChan <- true
		log.Printf("New leader elected: %s", monitor.currentLeader)

	case MSG_METADATA:
		receiveMetadata(payload, monitor)
	}
}

func notifyActualLeaderAndMetadata(idStr string, config Config, monitor *Monitor) {
	monitor.mu.Lock()
	leader := monitor.currentLeader
	monitor.mu.Unlock()
	senderID, _ := strconv.Atoi(strings.Split(idStr, "-")[1])
	addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount, senderID)
	if leader != "" {
		msg := append([]byte{MSG_COORDINATOR}, []byte(leader)...)
		communication.SendMessageToMonitors(addresses, msg)
	}
	sendMetadata(addresses, monitor)
}

type MetadataPayload struct {
	MonitorLastSeen map[string]time.Time `json:"monitor_last_seen"`
	WorkerLastSeen  map[string]time.Time `json:"worker_last_seen"`
}

func sendMetadata(addresses []*net.UDPAddr, monitor *Monitor) {
	monitor.mu.Lock()
	snap := MetadataPayload{
		MonitorLastSeen: monitor.monitorLastSeen,
		WorkerLastSeen:  monitor.workerLastSeen,
	}
	monitor.mu.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(snap); err != nil {
		log.Printf("Failed to encode metadata: %v", err)
		return
	}

	snapshotBytes := buf.Bytes()
	log.Printf("Sending metadata to %v (size: %d bytes)", addresses[0], len(snapshotBytes))
	msg := append([]byte{MSG_METADATA}, snapshotBytes...)
	communication.SendMessageToMonitors(addresses, msg)
}

func receiveMetadata(data string, monitor *Monitor) {
	var snap MetadataPayload
	buf := bytes.NewBuffer([]byte(data))
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&snap); err != nil {
		log.Printf("Failed to decode metadata: %v", err)
		return
	}
	log.Printf("Received METADATA: \n%+v", snap)

	monitor.mu.Lock()
	defer monitor.mu.Unlock()

	for k, v := range snap.MonitorLastSeen {
		if existing, ok := monitor.monitorLastSeen[k]; !ok || v.After(existing) {
			monitor.monitorLastSeen[k] = v
		}
	}

	for k, v := range snap.WorkerLastSeen {
		if existing, ok := monitor.workerLastSeen[k]; !ok || v.After(existing) {
			monitor.workerLastSeen[k] = v
		}
	}
}

func startElection(config Config, monitor *Monitor) {
	monitor.mu.Lock()
	if monitor.currentLeader != "" || monitor.electionInProgress {
		monitor.mu.Unlock()
		return
	}
	monitor.electionInProgress = true
	monitor.newCoordinatorChan = make(chan bool, 10)
	monitor.newCoordinatorChan = make(chan bool, 10)
	monitor.mu.Unlock()
	log.Printf(">>> Starting ELECTION (my ID: %d)", config.MonitorIdInt)

	// buscar nodos con IDs mas altos
	higher := []int{}
	for id := config.MonitorIdInt + 1; id <= config.monitorsCount; id++ {
		higher = append(higher, id)
	}

	// si no hay → soy el lider
	if len(higher) == 0 {
		log.Printf("No higher nodes exist, declaring myself leader")

		monitor.mu.Lock()
		monitor.currentLeader = config.MonitorId
		monitor.electionInProgress = false
		monitor.mu.Unlock()

		addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount)
		msg := append([]byte{MSG_COORDINATOR}, []byte(config.MonitorId)...)
		communication.SendMessageToMonitors(addresses, msg)
		log.Printf("★ I am the leader: %s", config.MonitorId)
		return
	}

	// enviar eleccion a los nodos mas altos
	addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount, higher...)
	log.Printf("Sending ELECTION to higher nodes: %v, addresses: %v", higher, addresses)
	msg := append([]byte{MSG_ELECTION}, []byte(config.MonitorId)...)
	communication.SendMessageToMonitors(addresses, msg)

	timeout := 3 * time.Second

	select {
	case <-monitor.okElectionMessageChan:
		log.Println("Received OK from higher node, dropping off of election")
		select {
		case <-monitor.newCoordinatorChan:
			log.Println("A new coordinator has been elected, dropping off of election")
			return
		case <-time.After(timeout):
			log.Println("No response after OK from higher nodes. Restarting election")
			monitor.mu.Lock()
			monitor.currentLeader = ""
			monitor.electionInProgress = false
			monitor.mu.Unlock()
			startElection(config, monitor)
		}
	case <-monitor.newCoordinatorChan:
		log.Println("A new coordinator has been elected, dropping off of election")
		return
	case <-time.After(timeout):
		log.Println("No response from higher nodes, declaring myself leader")

		monitor.mu.Lock()
		monitor.currentLeader = config.MonitorId
		monitor.electionInProgress = false
		monitor.mu.Unlock()

		addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount)
		msg := append([]byte{MSG_COORDINATOR}, []byte(config.MonitorId)...)
		communication.SendMessageToMonitors(addresses, msg)
		log.Printf("★ I am the leader: %s", config.MonitorId)
	}
}

func restartWorker(containerID string) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Printf("Docker client error: %v", err)
		return
	}
	defer cli.Close()

	err = cli.ContainerStart(context.Background(), containerID, container.StartOptions{})
	if err != nil {
		log.Printf("Restart error for %s: %v", containerID, err)
		return
	}

	log.Printf("✓ Restarted: %s", containerID)
}
