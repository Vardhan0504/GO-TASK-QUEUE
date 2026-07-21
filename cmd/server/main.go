package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Vardhan0504/GO-TASK-QUEUE/internal/queue"
	"github.com/Vardhan0504/GO-TASK-QUEUE/internal/web"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	concurrencyStr := os.Getenv("WORKER_CONCURRENCY")
	concurrency := 5
	if c, err := strconv.Atoi(concurrencyStr); err == nil && c > 0 {
		concurrency = c
	}

	// 1. Initialize Redis Client
	rdb, err := queue.NewRedisClient(redisURL)
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}
	log.Printf("Connected to Redis at %s", redisURL)

	// 2. Initialize WebSocket Hub
	hub := web.NewHub(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	// 3. Initialize Worker Pool
	wp := queue.NewWorkerPool(rdb, concurrency)
	wp.Start()

	// 4. Setup Routes & Static Asset Handler
	server := web.NewServer(rdb, hub)

	// WebSocket & API Routes
	http.HandleFunc("/ws", hub.ServeWS)
	http.HandleFunc("/api/enqueue", server.HandleEnqueue)
	http.HandleFunc("/api/stats", server.HandleGetStats)
	http.HandleFunc("/api/dlq", server.HandleGetDLQ)
	http.HandleFunc("/api/dlq/redrive", server.HandleRedriveDLQ)
	http.HandleFunc("/api/dlq/purge", server.HandlePurgeDLQ)

	// Serve CSS/JS static files from /public directory under /static/
	fs := http.FileServer(http.Dir("public"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	// Serve HTML Dashboard only on exact root "/" path
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "templates/index.html")
	})

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: http.DefaultServeMux,
	}

	// Graceful Shutdown Setup
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Server running on http://localhost:%s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-stop
	log.Println("Received termination signal. Shutting down gracefully...")

	// Stop Workers & Web Server
	wp.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced shutdown error: %v", err)
	}

	log.Println("Server exited cleanly.")
}
