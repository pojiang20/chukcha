package server

import (
	"github.com/chukcha/replication"
	"log"
	"sync"
	"time"
)

func NewOnDiskCreator(logger *log.Logger, dirName string, instanceName string, replStorage *replication.Storage, maxChunkSize uint64, rotateChunkInterval time.Duration) *OnDiskCreator {

}

type OnDiskCreator struct {
	logger              *log.Logger
	dirName             string
	instanceName        string
	replStorage         *replication.Storage
	maxChunkSize        uint64
	rotateChunkInterval time.Duration

	m        sync.Mutex
	storages map[string]*OnDisk
}
