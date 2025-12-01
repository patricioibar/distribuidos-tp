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

// ---------------- Config ---------------- //
type Config struct {
	Port         int
	MonitorId    string // ID of THIS monitor node
	MonitorIdInt int    // ID of THIS monitor node as int
	PeersCount   int    // total number of monitors
}

func getConfig() Config {
	monitorId := os.Getenv("MONITOR_ID")
	monitorIdInt, _ := strconv.Atoi(strings.Split(monitorId, "-")[1])
	port, _ := strconv.Atoi(os.Getenv("PORT"))
	peersCount, _ := strconv.Atoi(os.Getenv("PEERS_COUNT"))

	return Config{
		Port:         port,
		MonitorId:    monitorId,
		MonitorIdInt: monitorIdInt,
		PeersCount:   peersCount,
	}
}

// ---------------- Global State ---------------- //
var (
	mu sync.Mutex

	monitorLastSeen = map[string]time.Time{} // monitorID → lastSeen
	workerLastSeen  = map[string]time.Time{} // workerName → lastSeen

	currentLeader         string
	electionInProgress    bool
	okElectionMessageChan = make(chan bool, 10) // Buffered to prevent blocking
)

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

	addr := net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: config.Port,
	}

	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Fatalf("UDP listen error: %v", err)
	}
	defer conn.Close()

	go listenUDP(conn, config)
	go sendMonitorHeartbeat(config)

	// Initial election after brief delay
	time.Sleep(500 * time.Millisecond)
	go startElection(config)

	ticker := time.NewTicker(500 * time.Millisecond)
	timeout := 1 * time.Second

	for range ticker.C {
		mu.Lock()

		// ---- MONITOR FAILURE DETECTION ----
		for monitorID, last := range monitorLastSeen {
			if monitorID == config.MonitorId {
				continue // skip myself
			}

			if time.Since(last) > timeout {
				log.Printf("Monitor %s is DOWN", monitorID)

				// If the fallen monitor is the leader → start election
				if monitorID == currentLeader {
					log.Printf("Leader %s failed → starting election", monitorID)
					currentLeader = "" // Clear leader

					if !electionInProgress {
						electionInProgress = true
						mu.Unlock()
						startElection(config)
						if config.MonitorId == currentLeader {
							log.Printf("[Leader] Restarting monitor %s...", monitorID)
							go restartWorker(monitorID)
						}
						mu.Lock()
					}
				} else if config.MonitorId == currentLeader {
					// Only the leader can restart fallen monitors
					log.Printf("[Leader] Restarting monitor %s...", monitorID)
					mu.Unlock()
					go restartWorker(monitorID)
					mu.Lock()
				}

				delete(monitorLastSeen, monitorID)
			}
		}

		// ---- Leader handles worker failures ----
		if config.MonitorId == currentLeader {
			for workerID, last := range workerLastSeen {
				if time.Since(last) > timeout {
					log.Printf("[Leader] Worker %s DOWN → restarting...", workerID)
					mu.Unlock()
					go restartWorker(workerID)
					mu.Lock()
					delete(workerLastSeen, workerID)
				}
			}
		}

		mu.Unlock()
	}
}

// ---------------- Monitor Heartbeats ---------------- //
func sendMonitorHeartbeat(config Config) {
	t := time.NewTicker(250 * time.Millisecond)
	addresses, _ := communication.ResolveAddresses(config.MonitorId, config.PeersCount)

	for range t.C {
		communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_MONITOR, config.MonitorId))
	}
}

// ---------------- Message Handler ---------------- //
func listenUDP(conn *net.UDPConn, config Config) {
	buf := make([]byte, 1024)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		msg := string(buf[:n])
		handleMessage(msg, config)
	}
}

func handleMessage(msg string, config Config) {
	parts := split(msg, ":")
	if len(parts) < 2 {
		return
	}

	msgType := parts[0]
	idStr := parts[1]

	mu.Lock()
	defer mu.Unlock()

	switch msgType {

	// ------------- MONITOR HEARTBEAT -------------
	case MSG_MONITOR:
		monitorLastSeen[idStr] = time.Now()

	// ------------- WORKER HEARTBEAT --------------
	case MSG_WORKER:
		workerLastSeen[idStr] = time.Now()

	// ------------- BULLY ELECTION ----------------
	case MSG_ELECTION:
		log.Printf("Received ELECTION from %s", idStr)
		senderID, _ := strconv.Atoi(strings.Split(idStr, "-")[1])

		if senderID < config.MonitorIdInt {
			log.Printf("Sending OK to %s (I have higher ID)", idStr)
			addresses, _ := communication.ResolveAddresses(config.MonitorId, config.PeersCount, senderID)
			communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_OK, config.MonitorId))

			go startElection(config)

			// DON'T start election here - prevents election storm
			// Just send OK and let the Bully algorithm work
		}

	case MSG_OK:
		// Non-blocking send to channel
		select {
		case okElectionMessageChan <- true:
		default:
			// Channel full, that's okay
		}

	case MSG_COORDINATOR:
		senderID, _ := strconv.Atoi(strings.Split(idStr, "-")[1])

		if senderID >= config.MonitorIdInt {
			oldLeader := currentLeader
			currentLeader = idStr
			electionInProgress = false

			if oldLeader != idStr {
				log.Printf("New leader elected: %s", currentLeader)
			}

			// Non-blocking send
			/*select {
			case okElectionMessageChan <- true:
			default:
			}*/
		}
	}
}

// ---------------- Bully Election ---------------- //
func startElection(config Config) {
	log.Printf(">>> Starting ELECTION (my ID: %d)", config.MonitorIdInt)

	// Find nodes with higher IDs
	higher := []int{}
	for id := config.MonitorIdInt + 1; id <= config.PeersCount; id++ {
		higher = append(higher, id)
	}

	// If no higher nodes exist → I'm the leader
	if len(higher) == 0 {
		log.Printf("No higher nodes exist, declaring myself leader")

		mu.Lock()
		currentLeader = config.MonitorId
		electionInProgress = false
		mu.Unlock()

		addresses, _ := communication.ResolveAddresses(config.MonitorId, config.PeersCount)
		communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_COORDINATOR, config.MonitorId))
		log.Printf("★ I am the leader: %s", config.MonitorId)
		return
	}

	// Notify higher nodes
	addresses, _ := communication.ResolveAddresses(config.MonitorId, config.PeersCount, higher...)
	log.Printf("Sending ELECTION to higher nodes: %v, addresses: %v", higher, addresses)
	communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_ELECTION, config.MonitorId))

	// Wait for OK or timeout
	timeout := 1 * time.Second

	select {
	case <-okElectionMessageChan:
		log.Println("Received OK from higher node, waiting for COORDINATOR")
		// Higher node will handle it, we just wait
		return

	case <-time.After(timeout):
		log.Println("No response from higher nodes, declaring myself leader")

		mu.Lock()
		currentLeader = config.MonitorId
		electionInProgress = false
		mu.Unlock()

		addresses, _ := communication.ResolveAddresses(config.MonitorId, config.PeersCount)
		communication.SendMessageToMonitors(addresses, fmt.Sprintf("%s:%s", MSG_COORDINATOR, config.MonitorId))
		log.Printf("★ I am the leader: %s", config.MonitorId)
	}
}

// ---------------- Restart Worker ---------------- //
func restartWorker(containerID string) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Printf("Docker client error: %v", err)
		return
	}
	defer cli.Close()

	err = cli.ContainerRestart(context.Background(), containerID, container.StopOptions{})
	if err != nil {
		log.Printf("Restart error for %s: %v", containerID, err)
		return
	}

	log.Printf("✓ Restarted: %s", containerID)
}

// ---------------- String Split ---------------- //
func split(s, sep string) []string {
	var out []string
	curr := ""
	for _, ch := range s {
		if string(ch) == sep {
			out = append(out, curr)
			curr = ""
		} else {
			curr += string(ch)
		}
	}
	return append(out, curr)
}
