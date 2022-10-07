package replication

import (
	"log"
	"sync"
)

type Storage struct {
	logger          *log.Logger
	currentInstance string

	mu                sync.Mutex
	connectedReplicas map[string]chan Message
}
