package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitConn struct {
	conn *amqp.Connection
}

var (
	instance *RabbitConn
	once     sync.Once
)

func GetConnection(url string) *RabbitConn {
	once.Do(func() {
		connectExponentialRetry(url)
	})
	return instance
}

func connectExponentialRetry(url string) {
	var err error
	var c *amqp.Connection
	retryTime := 1
	for {
		c, err = amqp.Dial(url)
		if err == nil {
			break
		}
		log.Printf("No se pudo conectar a RabbitMQ: %v", err)
		retryTime *= 2
		time.Sleep(time.Duration(retryTime) * time.Second)
	}
	instance = &RabbitConn{conn: c}
}

func (r *RabbitConn) Channel() (*amqp.Channel, error) {
	if r.conn.IsClosed() {
		return nil, &MessageMiddlewareError{Code: MessageMiddlewareDisconnectedError, Msg: "Connection is closed"}
	}
	return r.conn.Channel()
}

func (r *RabbitConn) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

type QueueInfo struct {
	Name  string `json:"name"`
	VHost string `json:"vhost"`
}

func CleanupQueues(prefix, addr, username, password string) error {
	addr = strings.TrimRight(addr, "/")

	client := &http.Client{
		Timeout: 15 * time.Second,
	}

	// GET /api/queues
	getURL := fmt.Sprintf("%s/api/queues", addr)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, getURL, nil)
	if err != nil {
		return fmt.Errorf("creating request GET queues: %w", err)
	}
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("requesting GET /api/queues: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("GET /api/queues returned %s: %s", resp.Status, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response GET /api/queues: %w", err)
	}

	var queues []QueueInfo
	if err := json.Unmarshal(body, &queues); err != nil {
		return fmt.Errorf("parsing JSON /api/queues: %w", err)
	}

	var errs []string
	for _, q := range queues {
		if strings.HasPrefix(q.Name, prefix) {
			// vhost and name must be URL-escaped according to RabbitMQ API expectations
			vhostEscaped := url.PathEscape(q.VHost)
			nameEscaped := url.PathEscape(q.Name)
			delURL := fmt.Sprintf("%s/api/queues/%s/%s", addr, vhostEscaped, nameEscaped)

			// usá un contexto corto por cada delete
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			reqDel, err := http.NewRequestWithContext(ctx, http.MethodDelete, delURL, nil)
			if err != nil {
				errs = append(errs, fmt.Sprintf("creating request DELETE %s: %v", delURL, err))
				continue
			}
			reqDel.SetBasicAuth(username, password)

			respDel, err := client.Do(reqDel)
			if err != nil {
				errs = append(errs, fmt.Sprintf("DELETE %s error: %v", delURL, err))
				continue
			}
			io.Copy(io.Discard, respDel.Body)
			respDel.Body.Close()

			if respDel.StatusCode >= 300 {
				errs = append(errs, fmt.Sprintf("DELETE %s returned %s", delURL, respDel.Status))
				continue
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("some deletions failed:\n%s", strings.Join(errs, "\n"))
	}
	return nil
}
