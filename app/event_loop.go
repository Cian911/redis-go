package main

import (
	"errors"
	"net"
)

var (
	ErrQueueFull  = errors.New("Queue is full.")
	ErrQueueEmpty = errors.New("Queue is empty.")
)

type Queue interface {
	Enqueue(val event) error
	Dequeue() (event, error)
}

type EventQueue struct {
	channel chan event
}

type event struct {
	conn net.Conn
}

// Queue defines a list of tasks to awaiting to be processed
func NewQueue(capacity int) Queue {
	return &EventQueue{
		channel: make(chan event, capacity),
	}
}

func (q *EventQueue) Enqueue(val event) error {
	select {
	case q.channel <- val:
		return nil
	default:
		return ErrQueueFull
	}
}

func (q *EventQueue) Dequeue() (event, error) {
	select {
	case val := <-q.channel:
		return val, nil
	default:
		return event{}, ErrQueueEmpty
	}
}

// EventLoop takes tasks from the queue and sends them for processing
func EventLoop(q Queue) {
	for {
		ev, _ := q.Dequeue()
		go Process(ev)
	}
}

// Process takes a task and processes it
func Process(e event) {
	if e.conn == nil {
		// Do not proceed if we have no connection to respond to
		return
	}
	buffer := make([]byte, 64)

	e.conn.Read(buffer)
	e.conn.Write([]byte("+PONG\r\n"))
}
