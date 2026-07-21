package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type WorkerPool struct {
	rdb         *RedisClient
	concurrency int
	taskChan    chan *Task
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewWorkerPool initializes the worker pool structure
func NewWorkerPool(rdb *RedisClient, concurrency int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		rdb:         rdb,
		concurrency: concurrency,
		taskChan:    make(chan *Task, concurrency*2),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start launches worker goroutines, scheduler ticker, and queue fetcher
func (wp *WorkerPool) Start() {
	log.Printf("Starting worker pool with %d concurrent workers...", wp.concurrency)

	for i := 1; i <= wp.concurrency; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	go wp.schedulerLoop()
	go wp.fetcherLoop()
}

// Stop gracefully drains active workers and stops accepting new tasks
func (wp *WorkerPool) Stop() {
	log.Println("Stopping worker pool gracefully...")
	wp.cancel()
	close(wp.taskChan)
	wp.wg.Wait()
	log.Println("All workers safely stopped.")
}

// fetcherLoop pops pending tasks atomically via BRPOPLPUSH
func (wp *WorkerPool) fetcherLoop() {
	for {
		select {
		case <-wp.ctx.Done():
			return
		default:
			// Atomic pop from queue:pending and push to queue:processing
			res, err := wp.rdb.Client.BRPopLPush(wp.ctx, QueuePending, QueueProcessing, 2*time.Second).Result()
			if err != nil {
				if err == redis.Nil || wp.ctx.Err() != nil {
					continue
				}
				log.Printf("Error popping task: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			task, err := UnmarshalTask(res)
			if err != nil {
				log.Printf("Error parsing task payload: %v", err)
				continue
			}

			task.Status = StatusProcessing
			wp.publishEvent("TASK_STARTED", task)

			wp.taskChan <- task
		}
	}
}

// schedulerLoop promotes delayed retry tasks periodically
func (wp *WorkerPool) schedulerLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case <-ticker.C:
			moved, err := wp.rdb.PollDelayedTasks(wp.ctx)
			if err != nil && wp.ctx.Err() == nil {
				log.Printf("Error polling delayed tasks: %v", err)
			} else if moved > 0 {
				log.Printf("Promoted %d delayed retry tasks to pending queue", moved)
			}
		}
	}
}

// worker routine picks tasks from channel and manages execution/timeout
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	for task := range wp.taskChan {
		log.Printf("[Worker %d] Executing Task ID: %s (Type: %s, Attempt: %d/%d)",
			id, task.ID, task.Type, task.Retries+1, task.MaxRetries)

		timeout := time.Duration(task.TimeoutSec) * time.Second
		if timeout == 0 {
			timeout = 5 * time.Second
		}

		taskCtx, cancel := context.WithTimeout(wp.ctx, timeout)
		err := wp.executeTask(taskCtx, task)
		cancel()

		rawTask, _ := task.Marshal()
		// Remove from processing queue once finished/handled
		wp.rdb.Client.LRem(wp.ctx, QueueProcessing, 1, string(rawTask))

		if err != nil {
			log.Printf("[Worker %d] Task %s failed: %v", id, task.ID, err)
			wp.handleFailure(task, err)
		} else {
			log.Printf("[Worker %d] Task %s succeeded", id, task.ID)
			task.Status = StatusSuccess
			wp.publishEvent("TASK_COMPLETED", task)
		}
	}
}

// executeTask handles business logic or failure simulation
func (wp *WorkerPool) executeTask(ctx context.Context, task *Task) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1500 * time.Millisecond): // Simulate 1.5s execution work
		if task.Type == "FAIL_SIMULATION" {
			return fmt.Errorf("simulated system failure execution error")
		}
		return nil
	}
}

// handleFailure applies Exponential Backoff + Full Jitter or moves task to DLQ
func (wp *WorkerPool) handleFailure(task *Task, err error) {
	task.Retries++
	task.LastError = err.Error()

	if task.Retries >= task.MaxRetries {
		// Retries exhausted -> Move to DLQ
		task.Status = StatusDLQ
		raw, _ := task.Marshal()
		wp.rdb.Client.RPush(wp.ctx, QueueDLQ, string(raw))
		wp.publishEvent("TASK_DLQ", task)
		log.Printf("Task %s exhausted retries. Moved to Dead Letter Queue (DLQ).", task.ID)
	} else {
		// Exponential backoff calculation: Min(MaxDelay, Base * 2^retries) + Jitter
		base := 2.0
		backoffSec := int(math.Pow(base, float64(task.Retries))) + rand.Intn(2)
		task.ExecuteAt = time.Now().Add(time.Duration(backoffSec) * time.Second).Unix()
		task.Status = StatusPending

		raw, _ := task.Marshal()
		wp.rdb.Client.ZAdd(wp.ctx, QueueDelayed, redis.Z{
			Score:  float64(task.ExecuteAt),
			Member: string(raw),
		})
		wp.publishEvent("TASK_RETRY", task)
		log.Printf("Task %s scheduled for retry in %ds", task.ID, backoffSec)
	}
}

// publishEvent broadcasts real-time updates over Redis Pub/Sub channel
func (wp *WorkerPool) publishEvent(eventType string, task *Task) {
	msg := map[string]interface{}{
		"event": eventType,
		"task":  task,
		"time":  time.Now().Format("15:04:05"),
	}
	data, _ := json.Marshal(msg)
	wp.rdb.Client.Publish(wp.ctx, ChannelUpdates, string(data))
}