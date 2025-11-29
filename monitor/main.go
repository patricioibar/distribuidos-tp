package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

func main() {
	config := getConfig()

	log.Println("🚀 Monitor started. Listening for container events...")

	lastSeenMap := make(map[string]time.Time)
	mu := sync.Mutex{}

	port, err := strconv.Atoi(config.Port)
	if err != nil {
		log.Fatalf("Invalid port number: %v", err)
	}
	addr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP("0.0.0.0"),
	}

	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Fatalf("Error listening on UDP port %d: %v", port, err)
	}
	defer conn.Close()

	go func() {
		buf := make([]byte, 1024)
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				log.Printf("Error reading from UDP: %v", err)
				continue
			}
			//log.Printf("Received UDP message: %s", string(buf[:n]))
			mu.Lock()
			lastSeenMap[string(buf[:n])] = time.Now()
			mu.Unlock()
		}
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeout := 1 * time.Second

	for range ticker.C {
		now := time.Now()

		mu.Lock()
		for id, last := range lastSeenMap {
			if now.Sub(last) > timeout {
				log.Printf("%s is DOWN, restarting...", id)
				delete(lastSeenMap, id)
				go func(monitorId string) {
					err := restartWorker(monitorId)
					if err != nil {
						log.Printf("Failed to restart worker %s: %v", monitorId, err)
					} else {
						log.Printf("Successfully restarted worker %s", monitorId)
					}
				}(id)
			}
		}
		mu.Unlock()
	}

}

func restartWorker(containerId string) error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}

	err = cli.ContainerRestart(context.Background(), containerId, container.StopOptions{})
	if err != nil {
		return err
	}

	log.Printf("Container restarted: %s", containerId)
	return nil
}

func getConfig() Config {
	return Config{
		Port:      os.Getenv("PORT"),
		MonitorId: os.Getenv("MONITOR_ID"),
	}
}

type Config struct {
	Port      string
	MonitorId string
}
