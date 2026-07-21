package queue

import (
	"encoding/json"
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "PENDING"
	StatusProcessing TaskStatus = "PROCESSING"
	StatusSuccess    TaskStatus = "SUCCESS"
	StatusFailed     TaskStatus = "FAILED"
	StatusDLQ        TaskStatus = "DLQ"
)

type Task struct {
	ID         string     `json:"id"`
	Type       string     `json:"type"`
	Payload    string     `json:"payload"`
	MaxRetries int        `json:"max_retries"`
	Retries    int        `json:"retries"`
	TimeoutSec int        `json:"timeout_sec"`
	ExecuteAt  int64      `json:"execute_at"` // Unix epoch timestamp (seconds)
	Status     TaskStatus `json:"status"`
	LastError  string     `json:"last_error,omitempty"`
	CreatedAt  int64      `json:"created_at"`
}

// Marshal converts the Task struct into a JSON byte array.
func (t *Task) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

// UnmarshalTask parses a JSON string into a Task struct.
func UnmarshalTask(data string) (*Task, error) {
	var t Task
	err := json.Unmarshal([]byte(data), &t)
	return &t, err
}