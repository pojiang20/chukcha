package server

import (
	"github.com/chukcha/generics/heap"
	"log"
	"os"
	"sync"
	"time"
)

type StorageHooks interface {
}

// 在磁盘存储所有数据
type OnDisk struct {
	logger       *log.Logger
	dirname      string
	category     string
	instanceName string
	maxChunkSize uint64
	maxLineSize  uint64

	replicationDisabled bool

	repl StorageHooks

	downloadNotificationMu      sync.RWMutex
	downloadNotificationChanIdx int
	downloadNotificationChans   map[int]chan bool
	downloadNotificationSubs    heap.Min[replicationSub]
	downloadNotifications       map[string]*downloadNotification

	writeMu       sync.Mutex
	lastChunkFp   *os.File
	lastChunk     string
	lastChunkSize uint64
	lastChunkIdx  uint64
}

func NewOnDisk(logger *log.Logger, dirname, category, instanceName string, maxChunkSize uint64, maxLineSize uint64, rotateChunkInterval time.Duration, repl StorageHooks) (*OnDisk, error) {
	s := &OnDisk{
		logger:                    logger,
		dirname:                   dirname,
		category:                  category,
		instanceName:              instanceName,
		repl:                      repl,
		maxChunkSize:              maxChunkSize,
		maxLineSize:               maxLineSize,
		downloadNotifications:     make(map[string]*downloadNotification),
		downloadNotificationChans: make(map[int]chan bool),
		downloadNotificationSubs:  heap.NewMin[replicationSub](),
	}

	if err := s.initLastChunkIdx(dirname); err != nil {
		return nil, err
	}

	go s.createNextChunkThread(rotateChunkInterval)
}

type downloadNotification struct {
	chunk string
	size  uint64
}

type replicationSub struct {
	chunk string
	size  uint64
	ch    chan bool
}
