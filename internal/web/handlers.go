package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Vardhan0504/go-task-queue/internal/queue"
)

type Server struct {
	rdb *queue.RedisClient
	hub *Hub
}

func NewServer(rdb *queue.RedisClient, hub *Hub) *Server {
	return &Server{rdb: rdb, hub: hub}
}

// HandleEnqueue handles HTTP POST requests to enqueue new tasks
func (s *Server) HandleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskType := r.FormValue("task_type")
	payload := r.FormValue("payload")
	if taskType == "" {
		taskType = "GENERIC_JOB"
	}

	task := &queue.Task{
		ID:         fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Type:       taskType,
		Payload:    payload,
		MaxRetries: 3,
		Retries:    0,
		TimeoutSec: 5,
		Status:     queue.StatusPending,
		CreatedAt:  time.Now().Unix(),
	}

	raw, _ := task.Marshal()
	err := s.rdb.Client.RPush(r.Context(), queue.QueuePending, string(raw)).Err()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`<div class="p-3 bg-indigo-900/50 border border-indigo-700 text-indigo-200 text-xs rounded">Enqueued Task: %s</div>`, task.ID)))
}

// HandleGetStats returns JSON counts of pending, processing, delayed, and DLQ tasks
func (s *Server) HandleGetStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	pendingLen, _ := s.rdb.Client.LLen(ctx, queue.QueuePending).Result()
	processingLen, _ := s.rdb.Client.LLen(ctx, queue.QueueProcessing).Result()
	delayedLen, _ := s.rdb.Client.ZCard(ctx, queue.QueueDelayed).Result()
	dlqLen, _ := s.rdb.Client.LLen(ctx, queue.QueueDLQ).Result()

	stats := map[string]int64{
		"pending":    pendingLen,
		"processing": processingLen,
		"delayed":    delayedLen,
		"dlq":        dlqLen,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// HandleGetDLQ returns JSON list of all dead-lettered tasks
func (s *Server) HandleGetDLQ(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	items, err := s.rdb.Client.LRange(ctx, queue.QueueDLQ, 0, -1).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var tasks []*queue.Task
	for _, item := range items {
		t, err := queue.UnmarshalTask(item)
		if err == nil {
			tasks = append(tasks, t)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

// HandleRedriveDLQ moves all tasks from DLQ back to Pending Queue atomically
func (s *Server) HandleRedriveDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()

	count := 0
	for {
		item, err := s.rdb.Client.LPop(ctx, queue.QueueDLQ).Result()
		if err != nil {
			break
		}

		t, err := queue.UnmarshalTask(item)
		if err == nil {
			t.Retries = 0
			t.Status = queue.StatusPending
			t.LastError = ""
			raw, _ := t.Marshal()

			s.rdb.Client.RPush(ctx, queue.QueuePending, string(raw))
			count++
		}
	}

	w.WriteHeader(http.StatusOK)
	if count == 0 {
		w.Write([]byte("DLQ is empty. No tasks to redrive."))
	} else {
		w.Write([]byte(fmt.Sprintf("Successfully redrove %d task(s) from DLQ to Pending Queue", count)))
	}
}

// HandlePurgeDLQ permanently removes all failed messages from DLQ
func (s *Server) HandlePurgeDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()

	err := s.rdb.Client.Del(ctx, queue.QueueDLQ).Err()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Successfully purged all tasks from Dead Letter Queue."))
}