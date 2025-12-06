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

	//p "monitor/persistence"
	//"github.com/patricioibar/distribuidos-tp/persistance"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type NodeInfo struct {
	lastSeen time.Time
	state    string
}

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
	mu              sync.Mutex
	monitorLastSeen map[string]time.Time
	workerLastSeen  map[string]time.Time
	//nodeLastSeen          map[string]time.Time
	nodeLastSeen          map[string]*NodeInfo
	currentLeader         string
	electionInProgress    bool
	okElectionMessageChan chan bool
	otherNodes            []string
	//state 					persistance.StateManager
}

const (
	MSG_MONITOR     = "MONITOR"
	MSG_WORKER      = "WORKER"
	MSG_ELECTION    = "ELECTION"
	MSG_OK          = "OK"
	MSG_COORDINATOR = "COORDINATOR"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	config := getConfig()
	log.Printf("Node %s starting...", config.MonitorId)

	monitor := &Monitor{
		mu:              sync.Mutex{},
		monitorLastSeen: make(map[string]time.Time),
		workerLastSeen:  make(map[string]time.Time),
		//nodeLastSeen:          make(map[string]time.Time),
		nodeLastSeen:          make(map[string]*NodeInfo),
		okElectionMessageChan: make(chan bool, 1),
		currentLeader:         "",
		electionInProgress:    false,
		otherNodes:            strings.Split(os.Getenv("OTHER_NODES"), ","),
		//state:				   nil,
	}

	/*sm := persistance.InitStateManager(config.MonitorId)
	if sm == nil {
		log.Errorf("StateManager not initialized for monitor %s", config.MonitorId)
		return nil
	}
	monitor.state = sm

	persState, _ := monitor.state.GetState().(*p.PersistentState)

	op := p.NewAddKnownNodesOp(seq, data)
	if err := monitor.state.Log(op)*/

	addr := net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: config.Port,
	}

	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Fatalf("UDP listen error: %v", err)
	}
	defer conn.Close()

	for _, id := range monitor.otherNodes {
		if monitor.nodeLastSeen[id] == nil {
			monitor.nodeLastSeen[id] = &NodeInfo{}
		}
		//monitor.nodeLastSeen[id] = time.Now().Add(24 * time.Hour) //fecha ilogica para que si queda lo reinicie
		monitor.nodeLastSeen[id].lastSeen = time.Time{}
		monitor.nodeLastSeen[id].state = "NEVER_SEEN"
	}

	go listenUDP(conn, config, monitor)
	go communication.SendHeartbeatToMonitors(MSG_MONITOR, config.MonitorId, config.monitorsCount)

	// espero un poco para la primera eleccion
	//time.Sleep(500 * time.Millisecond)
	time.Sleep(2 * time.Second)
	startElection(config, monitor)

	ticker := time.NewTicker(500 * time.Millisecond)
	timeout := 1 * time.Second

	//iniciacion inicial de nodos caidos cuando empece
	monitor.mu.Lock()
	if monitor.currentLeader == config.MonitorId {
		for nodeID, info := range monitor.nodeLastSeen {
			if nodeID == config.MonitorId {
				continue
			}
			// if time.Since(info.lastSeen) < 0
			//if time.Since(info.lastSeen) > timeout && info.state == "NEVER_SEEN" {
			if info.lastSeen.IsZero() && info.state == "NEVER_SEEN" {
				monitor.nodeLastSeen[nodeID].state = "RESTARTING"
				log.Printf("[Leader] Node %s was DOWN on startup → restarting...", nodeID)
				//delete(monitor.nodeLastSeen, nodeID)
				monitor.mu.Unlock()
				go restartWorker(nodeID, monitor)
				monitor.mu.Lock()
			}
		}
		//time.Sleep(time.Millisecond * 500)
	}
	monitor.mu.Unlock()

	for range ticker.C {
		monitor.mu.Lock()
		leaderDown := false

		for nodeID, info := range monitor.nodeLastSeen {
			if time.Since(info.lastSeen) > timeout && info.state != "RESTARTING" {
				//fallenNodes = append(fallenNodes, nodeID)
				monitor.nodeLastSeen[nodeID].state = "DOWN"
				if strings.Contains(nodeID, "monitor") {
					log.Printf("Monitor %s DOWN", nodeID)
					if monitor.currentLeader == nodeID {
						leaderDown = true
					}
				} else {
					//log.Printf("Worker %s DOWN", nodeID)
				}
			} else if info.state == "RESTARTING" {
				//log.Printf("Restart already requested for node %s", nodeID)
			}
		}

		if leaderDown {
			log.Printf("Leader failed → starting election")
			monitor.currentLeader = ""
			if !monitor.electionInProgress {
				monitor.mu.Unlock()
				startElection(config, monitor)
				monitor.mu.Lock()
			}
		}

		for id, info := range monitor.nodeLastSeen {
			if info.state == "DOWN" {
				if config.MonitorId == monitor.currentLeader {
					monitor.nodeLastSeen[id].state = "RESTARTING"
					go func(node string) {
						restartWorker(node, monitor)
					}(id)
				}
			}
			//delete(monitor.nodeLastSeen, fallen)
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
		//monitor.workerLastSeen[idStr] = time.Now()
		info, exists := monitor.nodeLastSeen[idStr]
		if !exists || info == nil {
			info = &NodeInfo{}
			monitor.nodeLastSeen[idStr] = info
		}
		info.lastSeen = time.Now()
		info.state = "ALIVE"

	case MSG_WORKER:
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		//monitor.workerLastSeen[idStr] = time.Now()
		info, exists := monitor.nodeLastSeen[idStr]
		if !exists || info == nil {
			info = &NodeInfo{}
			monitor.nodeLastSeen[idStr] = info
		}
		info.lastSeen = time.Now()
		info.state = "ALIVE"

	case MSG_ELECTION:
		log.Printf("Received ELECTION from %s", idStr)
		senderID, _ := strconv.Atoi(strings.Split(idStr, "-")[1])

		if senderID < config.MonitorIdInt {
			log.Printf("Sending OK to %s (I have higher ID)", idStr)
			addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount, senderID)
			communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_OK, config.MonitorId))

			monitor.mu.Lock()
			defer monitor.mu.Unlock()
			if !monitor.electionInProgress {
				go startElection(config, monitor)
			}
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
			//monitor.electionInProgress = false

			select {
			case monitor.okElectionMessageChan <- true:
			default:
			}

			if oldLeader != idStr {
				log.Printf("New leader elected: %s", monitor.currentLeader)
			}
		}
		/*if senderID < config.MonitorIdInt {
			// I have a higher ID → I should be the leader
			log.Printf("Received COORDINATOR from lower ID %s, sending back my own COORDINATOR", idStr)
			addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount)
			communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_COORDINATOR, config.MonitorId))
			monitor.currentLeader = config.MonitorId
		} else {
			// accept the leader
			oldLeader := monitor.currentLeader
			monitor.currentLeader = idStr
			if oldLeader != idStr {
				log.Printf("New leader elected: %s", monitor.currentLeader)
			}
		}*/
		monitor.electionInProgress = false
	}
}

func startElection(config Config, monitor *Monitor) {
	monitor.mu.Lock()
	monitor.electionInProgress = true
	for len(monitor.okElectionMessageChan) > 0 {
		<-monitor.okElectionMessageChan
	}
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

	monitor.mu.Lock()
	if monitor.electionInProgress {
		monitor.mu.Unlock()
		// enviar eleccion a los nodos mas altos
		addresses, _ := communication.ResolveAddresses(config.MonitorId, config.monitorsCount, higher...)
		log.Printf("Sending ELECTION to higher nodes: %v, addresses: %v", higher, addresses)
		communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_ELECTION, config.MonitorId))

		timeout := 1 * time.Second

		select {
		case <-monitor.okElectionMessageChan:
			log.Println("Received OK from higher node, dropping off of election")
			monitor.mu.Lock()
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
			return
		}
	} else {
		monitor.mu.Unlock()
	}
}

func restartWorker(containerID string, monitor *Monitor) {
	/*monitor.mu.Lock()
	monitor.nodeLastSeen[containerID].state = "RESTARTING"
	monitor.mu.Unlock()*/

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
	/*monitor.mu.Lock()
	monitor.nodeLastSeen[containerID].lastSeen = time.Now()
	monitor.nodeLastSeen[containerID].state = "ALIVE"
	monitor.mu.Unlock()*/
}
