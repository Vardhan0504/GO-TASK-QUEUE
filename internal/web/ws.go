package web

import (
	"context"
	"log"
	"net/http"
	"sync"

	"github.com/Vardhan0504/go-task-queue/internal/queue"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Hub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan string
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
	rdb        *queue.RedisClient
}

func NewHub(rdb *queue.RedisClient) *Hub {
	return &Hub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan string),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		rdb:        rdb,
	}
}

// Run listens for Redis Pub/Sub events and broadcasts them to all connected WebSocket clients
func (h *Hub) Run(ctx context.Context) {
	pubsub := h.rdb.Client.Subscribe(ctx, queue.ChannelUpdates)
	defer pubsub.Close()

	ch := pubsub.Channel()

	go func() {
		for msg := range ch {
			h.broadcast <- msg.Payload
		}
	}()

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				client.Close()
			}
			h.mu.Unlock()
		case message := <-h.broadcast:
			h.mu.Lock()
			for client := range h.clients {
				err := client.WriteMessage(websocket.TextMessage, []byte(message))
				if err != nil {
					client.Close()
					delete(h.clients, client)
				}
			}
			h.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// ServeWS upgrades the HTTP connection to a WebSocket connection
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS upgrade error: %v", err)
		return
	}
	h.register <- conn
}
