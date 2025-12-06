package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

func main() {
	if len(os.Args) != 6 {
		fmt.Println("Usage: container-killer <min_number_of_containers_to_kill_each_interval> <max_number_of_containers_to_kill_each_interval> <monitors_count> <min_interval_ms> <max_interval_ms>")
		return
	}

	minIntervalMs, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Fatal("Invalid minimum interval:", err)
	}

	maxIntervalMs, err := strconv.Atoi(os.Args[5])
	if err != nil {
		log.Fatal("Invalid maximum interval:", err)
	}

	rand.Seed(time.Now().UnixNano())

	fmt.Println("🔥 Container killer started")
	fmt.Printf("   Kills %s-%s random running containers every %vms-%vms\n", os.Args[1], os.Args[2], os.Args[4], os.Args[5])
	fmt.Printf("   Monitors count: %s\n", os.Args[3])

	ctx := context.Background()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatal("Failed to connect to Docker:", err)
	}

	killCountMin, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("Invalid min number for containers to kill:", err)
	}

	killCountMax, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal("Invalid max number for containers to kill:", err)
	}

	monitorsCount, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatal("Invalid number for monitors count:", err)
	}

	killingLoop(ctx, maxIntervalMs, minIntervalMs, killCountMin, killCountMax, monitorsCount, cli)
}

func killingLoop(ctx context.Context, maxIntervalMs int, minIntervalMs int, killCountMin int, killCountMax int, monitorsCount int, cli *client.Client) {
	for {
		containers, err := cli.ContainerList(ctx, container.ListOptions{})
		if err != nil {
			log.Println("Failed to list containers:", err)
			return
		}
		var wg sync.WaitGroup
		interval := time.Duration(rand.Intn(maxIntervalMs-minIntervalMs)+minIntervalMs) * time.Millisecond

		killCount := rand.Intn(killCountMax-killCountMin+1) + killCountMin

		deathList := fillDeathList(killCount, containers, monitorsCount)
		fmt.Printf("Deathlist: %v\n", deathList)
		deathRow := make(chan string, len(deathList))

		for _, id := range deathList {
			deathRow <- id
		}
		close(deathRow)

		for range deathList {
			wg.Add(1)
			go func(deathRow chan string, ctx context.Context, cli *client.Client) {
				defer wg.Done()
				killContainer(deathRow, ctx, cli)
			}(deathRow, ctx, cli)
		}
		wg.Wait() // esperar a matar a todos para seguir

		fmt.Printf("Waiting for %v before killing again...\n", interval)
		time.Sleep(interval)
	}
}

func fillDeathList(killCount int, containers []container.Summary, monitorsCount int) []string {
	fmt.Printf("Selecting %d containers to kill...\n", killCount)
	deathList := make([]string, 0, killCount)
	chosenMonitors := 0
	runningMonitors := 0

	for i := range containers {
		name := containers[i].Names[0]
		if strings.Contains(name, "/monitor") && containers[i].State == "running" {
			runningMonitors++
		}
	}

	for i := 0; i < killCount; i++ {
		// 1 - elegir un contenedor random y si ya esta en deathlist o no esta corriendo elegir otro
		index := rand.Intn(len(containers))
		name := containers[index].Names[0]

		if slices.Contains(deathList, name) || containers[index].State != "running" {
			log.Printf("⚠️ Container %s already selected or not running, picking another...\n", containers[index].Names[0])
			i--
			continue
		}
		// 2 - si es /analyst o /monitor o /rabbitmq elegir otro
		if strings.Contains(name, "/analyst") || strings.Contains(name, "/rabbitmq") || strings.Contains(name, "/coffee-analyzer") {
			log.Printf("☕ Skipping analyst or rabbitmq or monitor containers: %s\n", name)
			i--
			continue
		}
		// 3 - si elijo un monitor y ya estan todos elegidos elegir otro contenedor
		if strings.Contains(name, "/monitor") {
			chosenMonitors++
			if chosenMonitors >= runningMonitors {
				// ⚠️ Can't kill every monitor, pick another container
				chosenMonitors--
				i--
				continue
			}
		}
		// 4 - si paso todos los chequeos lo agrego a la deathlist
		deathList = append(deathList, name)
	}

	return deathList
}

func killContainer(deathRow chan string, ctx context.Context, cli *client.Client) {
	name := <-deathRow
	fmt.Printf("𖦏 Targeted container to kill: %s\n", name)

	if err := cli.ContainerKill(ctx, name, "SIGKILL"); err != nil {
		log.Printf("❌ Failed to kill %s: %v\n", name, err)
		return
	}

	fmt.Printf("☠️ Successfully killed %s\n", name)
}
