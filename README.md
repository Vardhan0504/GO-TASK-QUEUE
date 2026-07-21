# 🚀 Distributed Task Queue Engine in Go

A high-performance, asynchronous distributed task queue engine built with **Go**, **Redis Lua scripts**, **WebSockets**, and **HTMX**. Inspired by Celery and Asynq, this project features a worker pool, exponential backoff retries, Dead Letter Queue (DLQ) redrive capabilities, and a real-time monitoring dashboard.

---

## 🌟 Features

* **Concurrent Worker Pool:** Configurable worker routines processing tasks concurrently.
* **Atomic Redis Operations:** Transactional queue management (Pending, Processing, Delayed, DLQ) using custom Redis Lua scripts.
* **Exponential Backoff & Retries:** Automatic retries for transient failures before routing poison messages to the Dead Letter Queue.
* **DLQ Management:** Real-time visibility with full **Redrive All** and **Purge All** administrative controls.
* **Real-time Event Streaming:** WebSockets stream task lifecycle events (`TASK_STARTED`, `TASK_COMPLETED`, `TASK_RETRY`, `TASK_DLQ`) directly to the browser.
* **Lightweight UI:** Modern dashboard powered by **Tailwind CSS** and **HTMX** for seamless server-driven UI updates without heavy JavaScript frameworks.

---

## 🏗️ System Architecture

```text
               +-------------------+
               |  Producer Console |
               +---------+---------+
                         | (HTTP POST /api/enqueue)
                         v
               +-------------------+
               |   Pending Queue   | <--- [Redis List]
               +---------+---------+
                         |
                         | (Worker Pop / Lua Atomic Transfer)
                         v
               +-------------------+
               |  Processing Queue | <--- [Redis List]
               +---------+---------+
                         |
           +-------------+-------------+
           |                           |
    (Success)                      (Failure)
           v                           v
     [Task Done]             +-------------------+
                             |   Delayed ZSET    | (Exponential Backoff)
                             +---------+---------+
                                       |
                                (Max Retries Reached)
                                       v
                             +-------------------+
                             | Dead Letter Queue | (DLQ)
                             +-------------------+
