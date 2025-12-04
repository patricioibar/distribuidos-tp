package main

import (
	"communication"
	"context"
	"fmt"
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
}

const (
	MSG_MONITOR     = "MONITOR"
	MSG_WORKER      = "WORKER"
	MSG_ELECTION    = "ELECTION"
	MSG_OK          = "OK"
	MSG_COORDINATOR = "COORDINATOR"
)

func main() {
	config := getConfig()
	log.Printf("Node %s starting...", config.MonitorId)

	monitor := &Monitor{
		mu:                    sync.Mutex{},
		monitorLastSeen:       make(map[string]time.Time),
		workerLastSeen:        make(map[string]time.Time),
		okElectionMessageChan: make(chan bool, 10),
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
	go communication.SendHeartbeatToMonitors(MSG_MONITOR, config.MonitorId, config.monitorsCount)

	// espero un poco para la primera eleccion
	time.Sleep(500 * time.Millisecond)
	startElection(config, monitor)

	ticker := time.NewTicker(500 * time.Millisecond)
	timeout := 1 * time.Second

	for range ticker.C {
		monitor.mu.Lock()

		// resolver fallas de monitores
		for monitorID, last := range monitor.monitorLastSeen {
			if monitorID == config.MonitorId {
				continue
			}

			if time.Since(last) > timeout {
				log.Printf("Monitor %s is DOWN", monitorID)
				delete(monitor.monitorLastSeen, monitorID)

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
					monitor.mu.Unlock()
					go restartWorker(monitorID)
					monitor.mu.Lock()
				}
			}
		}

		// resolver fallas de workers
		if config.MonitorId == monitor.currentLeader {
			for workerID, last := range monitor.workerLastSeen {
				if time.Since(last) > timeout {
					log.Printf("[Leader] Worker %s DOWN → restarting...", workerID)
					delete(monitor.workerLastSeen, workerID)
					monitor.mu.Unlock()
					go restartWorker(workerID)
					monitor.mu.Lock()
				}
			}
		}

		monitor.mu.Unlock()
	}
}

// ---------------- Message Handler ---------------- //
func listenUDP(conn *net.UDPConn, config Config, monitor *Monitor) {
	buf := make([]byte, 1024)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		msg := string(buf[:n])
		handleMessage(msg, config, monitor)
	}
}

func handleMessage(msg string, config Config, monitor *Monitor) {
	parts := strings.Split(msg, ":")
	if len(parts) < 2 {
		return
	}

	msgType := parts[0]
	idStr := parts[1]

	switch msgType {

	case MSG_MONITOR:
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		monitor.monitorLastSeen[idStr] = time.Now()

	case MSG_WORKER:
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		monitor.workerLastSeen[idStr] = time.Now()

	case MSG_ELECTION:
		log.Printf("Received ELECTION from %s", idStr)
		senderID, _ := strconv.Atoi(strings.Split(idStr, "-")[1])

		if senderID < config.MonitorIdInt {
			log.Printf("Sending OK to %s (I have higher ID)", idStr)
			addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount, senderID)
			communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_OK, config.MonitorId))

			go startElection(config, monitor)
		}

	case MSG_OK:
		select {
		case monitor.okElectionMessageChan <- true:
		default:
		}

	case MSG_COORDINATOR:
		senderID, _ := strconv.Atoi(strings.Split(idStr, "-")[1])

		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		if senderID >= config.MonitorIdInt {
			oldLeader := monitor.currentLeader
			monitor.currentLeader = idStr
			monitor.electionInProgress = false

			if oldLeader != idStr {
				log.Printf("New leader elected: %s", monitor.currentLeader)
			}
		}
	}
}

func startElection(config Config, monitor *Monitor) {
	monitor.mu.Lock()
	monitor.electionInProgress = true
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
		communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_COORDINATOR, config.MonitorId))
		log.Printf("★ I am the leader: %s", config.MonitorId)
		return
	}

	// enviar eleccion a los nodos mas altos
	addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount, higher...)
	log.Printf("Sending ELECTION to higher nodes: %v, addresses: %v", higher, addresses)
	communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_ELECTION, config.MonitorId))

	timeout := 3 * time.Second

	select {
	case <-monitor.okElectionMessageChan:
		log.Println("Received OK from higher node, dropping off of election")
		monitor.mu.Lock()
		monitor.currentLeader = config.MonitorId
		monitor.electionInProgress = false
		monitor.mu.Unlock()
		return

	case <-time.After(timeout):
		log.Println("No response from higher nodes, declaring myself leader")

		monitor.mu.Lock()
		monitor.currentLeader = config.MonitorId
		monitor.electionInProgress = false
		monitor.mu.Unlock()

		addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount)
		communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_COORDINATOR, config.MonitorId))
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
