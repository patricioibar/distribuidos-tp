package main

import (
	"communication"
	"context"
	"encoding/binary"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Port          int
	MonitorId     string
	MonitorIdInt  int
	monitorsCount int
	NodesList     []string
}

func getConfig() Config {
	monitorId := os.Getenv("MONITOR_ID")
	monitorIdInt, _ := strconv.Atoi(strings.Split(monitorId, "-")[1])
	port, _ := strconv.Atoi(os.Getenv("PORT"))
	monitorsCount, _ := strconv.Atoi(os.Getenv("MONITORS_COUNT"))
	nodesListStr := os.Getenv("NODES_LIST")
	nodesList := []string{}
	if nodesListStr != "" {
		nodesList = strings.Split(nodesListStr, ",")
	}

	return Config{
		Port:          port,
		MonitorId:     monitorId,
		MonitorIdInt:  monitorIdInt,
		monitorsCount: monitorsCount,
		NodesList:     nodesList,
	}
}

type Monitor struct {
	mu                     sync.Mutex
	monitorLastSeen        map[string]time.Time
	workerLastSeen         map[string]time.Time
	monitorRestartCooldown map[string]time.Time
	workerRestartCooldown  map[string]time.Time
	restartInProgress      map[string]bool
	currentLeader          string
	electionInProgress     bool
	okElectionMessageChan  chan bool
	newCoordinatorChan     chan bool
}

const (
	MSG_MONITOR           byte = 1
	MSG_WORKER            byte = 2
	MSG_ELECTION          byte = 3
	MSG_OK                byte = 4
	MSG_COORDINATOR       byte = 5
	MSG_ASK_ACTUAL_LEADER byte = 6
	MSG_METADATA          byte = 7
)

const (
	TIMEOUT = 1 * time.Second
)

func main() {
	config := getConfig()
	log.Printf("Starting...")

	monitorLastSeen := make(map[string]time.Time)
	workerLastSeen := make(map[string]time.Time)

	for _, v := range config.NodesList {
		if strings.Contains(v, "monitor") {
			monitorLastSeen[v] = time.Now().Add(TIMEOUT * 2)
		} else {
			workerLastSeen[v] = time.Now().Add(TIMEOUT * 2)
		}
	}

	monitor := &Monitor{
		mu:                     sync.Mutex{},
		monitorLastSeen:        monitorLastSeen,
		workerLastSeen:         workerLastSeen,
		monitorRestartCooldown: make(map[string]time.Time),
		workerRestartCooldown:  make(map[string]time.Time),
		restartInProgress:      make(map[string]bool),
		okElectionMessageChan:  make(chan bool, 10),
		newCoordinatorChan:     make(chan bool, 10),
		currentLeader:          "",
		electionInProgress:     false,
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
	askForActualLeader(config)
	
	// Wait for coordinator response with timeout
	select {
	case <-monitor.newCoordinatorChan:
		monitor.mu.Lock()
		log.Printf("Discovered existing leader: %s, skipping election", monitor.currentLeader)
		monitor.mu.Unlock()
	case <-time.After(2 * time.Second):
		monitor.mu.Lock()
		if monitor.currentLeader == "" {
			monitor.mu.Unlock()
			log.Printf("No leader response, starting election")
			startElection(config, monitor)
		} else {
			log.Printf("Leader set during wait: %s", monitor.currentLeader)
			monitor.mu.Unlock()
		}
	}

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
						monitor.mu.Lock()
						if !monitor.restartInProgress[monitorID] {
							log.Printf("[Leader] Restarting monitor %s...", monitorID)
							monitor.monitorRestartCooldown[monitorID] = time.Now()
							monitor.restartInProgress[monitorID] = true
							monitor.mu.Unlock()
							go restartWorkerWithCallback(monitorID, monitor)
						} else {
							monitor.mu.Unlock()
						}
					} else {
						log.Printf("Not the leader (%s), can't restart monitor %s", config.MonitorId, monitorID)
					}
					monitor.mu.Lock()
				}
				} else if config.MonitorId == monitor.currentLeader {
					// solo el lider puede reiniciar
					if lastRestart, exists := monitor.monitorRestartCooldown[monitorID]; !exists || time.Since(lastRestart) > TIMEOUT {
						log.Printf("[Leader] Restarting monitor %s...", monitorID)
						monitor.monitorRestartCooldown[monitorID] = time.Now()
						go restartWorker(monitorID)
					}
				}
			}
		}

		// resolver fallas de workers
		if config.MonitorId == monitor.currentLeader {
			for workerID, last := range monitor.workerLastSeen {
				if time.Since(last) > TIMEOUT {
					if !monitor.restartInProgress[workerID] {
						// check cooldown to prevent rapid restarts
						if lastRestart, exists := monitor.workerRestartCooldown[workerID]; !exists || time.Since(lastRestart) > TIMEOUT {
							log.Printf("[Leader] Worker %s DOWN → restarting...", workerID)
							monitor.workerRestartCooldown[workerID] = time.Now()
							monitor.restartInProgress[workerID] = true
							go restartWorkerWithCallback(workerID, monitor)
						}
					}
				}
			}
		}

		monitor.mu.Unlock()
	}
}

func askForActualLeader(config Config) {
	addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount)
	msg := append([]byte{MSG_ASK_ACTUAL_LEADER}, []byte(config.MonitorId)...)
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
	lenTimeBytes := msg[:4]
	lenTime := binary.BigEndian.Uint32(lenTimeBytes)
	timeBytes := msg[4 : 4+lenTime]
	var timestamp time.Time
	err := timestamp.UnmarshalBinary(timeBytes)
	if err != nil {
		log.Printf("Error unmarshaling time: %v", err)
		return
	}
	if time.Since(timestamp) > TIMEOUT {
		log.Printf("Stale message received, ignoring")
		return
	}

	msgType := msg[4+lenTime]
	payload := msg[5+lenTime:]

	switch msgType {

	case MSG_MONITOR:
		monitor.mu.Lock()
		monitor.monitorLastSeen[string(payload)] = time.Now()
		monitor.mu.Unlock()

	case MSG_WORKER:
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		monitor.workerLastSeen[string(payload)] = time.Now()

	case MSG_ASK_ACTUAL_LEADER:
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		if monitor.currentLeader == config.MonitorId {
			go notifyActualLeader(string(payload), config, monitor)
		}

	case MSG_ELECTION:
		payloadStr := string(payload)
		log.Printf("Received ELECTION from %s", payloadStr)
		senderID, _ := strconv.Atoi(strings.Split(payloadStr, "-")[1])
		monitor.mu.Lock()
		monitor.currentLeader = ""
		monitor.monitorLastSeen[string(payload)] = time.Now()
		monitor.mu.Unlock()

		if senderID < config.MonitorIdInt {
			log.Printf("Sending OK to %s (I have higher ID)", payloadStr)
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
		monitor.monitorLastSeen[string(payload)] = time.Now()
		monitor.currentLeader = string(payload)
		monitor.electionInProgress = false
		monitor.mu.Unlock()
		select {
		case monitor.newCoordinatorChan <- true:
		default:
		}
		log.Printf("New leader elected: %s", monitor.currentLeader)
	}
}

func notifyActualLeader(idStr string, config Config, monitor *Monitor) {
	monitor.mu.Lock()
	leader := monitor.currentLeader
	monitor.mu.Unlock()
	senderID, _ := strconv.Atoi(strings.Split(idStr, "-")[1])
	addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount, senderID)
	if leader != "" {
		msg := append([]byte{MSG_COORDINATOR}, []byte(leader)...)
		communication.SendMessageToMonitors(addresses, msg)
	}
}

func startElection(config Config, monitor *Monitor) {
	monitor.mu.Lock()
	if monitor.currentLeader != "" || monitor.electionInProgress {
		monitor.mu.Unlock()
		return
	}
	monitor.electionInProgress = true
	monitor.okElectionMessageChan = make(chan bool, 10)
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

	timeout := 1 * time.Second

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "start", containerID)
	output, err := cmd.CombinedOutput()

	if ctx.Err() == context.DeadlineExceeded {
		log.Printf("Restart timeout for %s", containerID)
		return
	}

	if err != nil {
		log.Printf("Restart error for %s: %v, output: %s", containerID, err, string(output))
		return
	}

	log.Printf("✓ Restarted: %s, output: %s", containerID, string(output))
	
	// Verify container is actually running after start
	time.Sleep(100 * time.Millisecond)
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer checkCancel()
	
	checkCmd := exec.CommandContext(checkCtx, "docker", "inspect", "-f", "{{.State.Running}}", containerID)
	checkOutput, checkErr := checkCmd.CombinedOutput()
	
	if checkErr != nil {
		log.Printf("Failed to verify %s state: %v", containerID, checkErr)
		return
	}
	
	running := strings.TrimSpace(string(checkOutput))
	if running != "true" {
		log.Printf("⚠ Container %s started but crashed immediately (running=%s)", containerID, running)
	}
}

func restartWorkerWithCallback(containerID string, monitor *Monitor) {
	defer func() {
		monitor.mu.Lock()
		delete(monitor.restartInProgress, containerID)
		monitor.mu.Unlock()
	}()
	
	restartWorker(containerID)
}
