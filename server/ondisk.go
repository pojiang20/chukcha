package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/chukcha/generics/heap"
	"github.com/chukcha/protocol"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var errBufTooSmall = errors.New("the buffer is too small to contain a simgle message")

const AcknowledgedSuffix = ".acknowledged"

// TODO 这里是不是相当于回调函数？
type StorageHooks interface {
	AfterCreatingChunk(ctx context.Context, category string, fileName string)
	AfterAcknowledgeChunk(ctx context.Context, category string, fileName string)
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

func (s *OnDisk) initLastChunkIdx(dirname string) error {
	files, err := os.ReadDir(dirname)
	if err != nil {
		return fmt.Errorf("readdir(%q): %v", dirname, err)
	}

	for _, fi := range files {
		instance, chunkIdx := protocol.ParseChunkFileName(fi.Name())
		if chunkIdx < 0 || instance != s.instanceName {
			continue
		}

		if uint64(chunkIdx)+1 >= s.lastChunkIdx {
			s.lastChunkIdx = uint64(chunkIdx) + 1
		}
	}

	return nil
}

func (s *OnDisk) createNextChunkThread(interval time.Duration) {
	for {
		time.Sleep(interval)

		if err := s.tryCreateNextEmptyChunkIfNeeded(); err != nil {
			//TODO 村logger指针和直接使用log.printf有什么区别？
			s.logger.Printf("Creating next empty chunk is backgroud failed: %v", err)
		}
	}
}

func (s *OnDisk) tryCreateNextEmptyChunkIfNeeded() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.lastChunkSize == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	//如果在超时前操作完成，则cancel用于主动释放资源
	defer cancel()

	s.logger.Printf("Creating empty chunk from timer")

	return s.createNextChunk(ctx)
}

func (s *OnDisk) createNextChunk(ctx context.Context) error {
	if s.lastChunkFp != nil {
		s.lastChunkFp.Close()
		s.lastChunkFp = nil
	}

	newChunk := fmt.Sprintf("%s-chunk%09d", s.instanceName, s.lastChunkIdx)

	fp, err := os.OpenFile(filepath.Join(s.dirname, newChunk), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer fp.Close()

	s.lastChunk = newChunk
	s.lastChunkSize = 0
	s.lastChunkIdx++

	if !s.replicationDisabled {
		s.repl.AfterCreatingChunk(ctx, s.category, s.lastChunk)
	}
	return nil
}

// 如果msg无法写入chunk的剩余空间，则建立一个新的chunk来写入msg
func (s *OnDisk) Write(ctx context.Context, msgs []byte) (chunkName string, off int64, err error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if len(msgs) > 0 && msgs[len(msgs)-1] != '\n' {
		return "", 0, fmt.Errorf("messages body must end with new line character")
	}

	for _, ln := range bytes.Split(msgs, []byte{'\n'}) {
		if len(ln) > int(s.maxLineSize) {
			return "", 0, fmt.Errorf("processing message %q: messages must not exceed %d bytes", s.shortenLine(ln), s.maxLineSize)
		}
	}

	willExceedMaxChunkSize := s.lastChunkSize+uint64(len(msgs)) > s.maxChunkSize

	if s.lastChunk == "" || (s.lastChunkSize > 0 && willExceedMaxChunkSize) {
		if err := s.createNextChunk(ctx); err != nil {
			return "", 0, fmt.Errorf("creating next chunk %v", err)
		}
	}

	fp, err := s.getLastChunkFp()
	if err != nil {
		return "", 0, err
	}

	n, err := fp.Write(msgs)
	s.lastChunkSize += uint64(n)

	if err != nil {
		return "", 0, err
	}

	return s.lastChunk, int64(s.lastChunkSize), nil
}

func (s *OnDisk) Read(chunk string, off uint64, maxSize uint64, w io.Writer) error {
	//通过纯词法处理，返回最短等效的路径名
	chunk = filepath.Clean(chunk)
	_, err := os.Stat(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	fp, err := os.Open(filepath.Join(s.dirname, chunk))
	if err != nil {
		return fmt.Errorf("Open(%q): %w", chunk, err)
	}
	defer fp.Close()

	buf := make([]byte, maxSize)
	n, err := fp.ReadAt(buf, int64(off))

	if n == 0 {
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}

	//TODO 不完整消息的具体情况是？
	//如果最后一条消息不完整，则不发送
	truncated, _, err := cutToLastMessage(buf[0:n])
	if err != nil {
		return err
	}
	if _, err := w.Write(truncated); err != nil {
		return err
	}
	return nil
}

func (s *OnDisk) isLastChunk(chunk string) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return chunk == s.lastChunk
}

func (s *OnDisk) Ack(ctx context.Context, chunk string, size uint64) error {
	if s.isLastChunk(chunk) {
		return fmt.Errorf("could not delete incomplete chunk %q", chunk)
	}

	chunkFilename := filepath.Join(s.dirname, chunk)

	fi, err := os.Stat(chunkFilename)
	if err != nil {
		return fmt.Errorf("stat %q: %w", chunk, err)
	}

	if uint64(fi.Size()) > size {
		return fmt.Errorf("file was not fully processed: the supplied processed size %d is smaller than the chunk file size %d", size, fi.Size())
	}

	if err := s.doAckChunk(chunk); err != nil {
		return fmt.Errorf("ack %q: %v", chunkFilename, err)
	}

	s.repl.AfterAcknowledgeChunk(ctx, s.category, chunkFilename)
	return nil
}

func (s *OnDisk) AckDirect(chunk string) error{
	if err := s.doAckChunk(chunk); err != nil && !errors.Is(err,os.ErrNotExist){
		return fmt.Errorf("removing %q: %v",chunk,err)
	}
	return nil
}

func (s *OnDisk) ReplicationAck(ctx context.Context,chunk,instance string,size uint64) error{
	s.downloadNotificationMu.Lock()
	defer s.downloadNotificationMu.Unlock()

	s.downloadNotifications[instance] = &downloadNotification{
		chunk:chunk,
		size: size,
	}

	for _,ch := range s.downloadNotificationChans{
		select {
		case ch<-true:
		default:
		}
	}

	for s.downloadNotificationSubs.Len() >0{
		r := s.downloadNotificationSubs.Pop()

		if (chunk == r.chunk && size >= r.size) || chunk>r.chunk{
			select {
			case r.ch <- true
			default:
			}
		}else{
			s.downloadNotificationSubs.Push(r)
			break
		}
	}
	return nil
}


func (s *OnDisk) doAckChunk(chunk string) error {
	chunkFilename := filepath.Join(s.dirname, chunk)

	fp, err := os.OpenFile(chunkFilename, os.O_WRONLY, 0666)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get file descriptor for ack operation for chunk %q: %v", chunk, err)
	}
	defer fp.Close()

	if err := fp.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate file %q: %v", chunk, err)
	}

	return nil
}

func (s *OnDisk) Wait(ctx context.Context,chunkName string,off uint64,minSyncReplicas uint) error{

	return nil
}

func (s *OnDisk) ListChunks() ([]protocol.Chunk,error){
	var res []protocol.Chunk

	dis,err := os.ReadDir(s.dirname)
	if err != nil {
		return nil,err
	}

	for idx,di := range dis{
		fi,err := di.Info()
		if errors.Is(err,os.ErrNotExist){
			continue
		}else if err!=nil{
			return nil,fmt.Errorf("reading directory: %v",err)
		}

		if strings.HasSuffix(di.Name(),AcknowledgedSuffix){
			continue
		}

		instanceName,_ := protocol.ParseChunkFileName(di.Name())

		c := protocol.Chunk{
			Name: di.Name(),
			Complete: true,
			Size: uint64(fi.Size()),
		}

		//TODO 分析一下具体情况
		//存在不完整的chunk
		if idx == len(dis)-1{
			c.Complete = false
		}else if nextInstance,_ := protocol.ParseChunkFileName(dis[idx+1].Name();nextInstance!= instanceName){
			c.Complete = false
		}
		res = append(res,c)
	}
	return res,nil
}

func (s *OnDisk) Wait(ctx context.Context,chunkName string,off uint64,minSyncReplicas uint) error{
	var ch chan bool

	if minSyncReplicas == 1 {
		r := replicationSub{
			chunk:chunkName,
			size: off,
			ch: make(chan bool,2),
		}

		s.downloadNotificationMu.Lock()
		s.downloadNotificationSubs.Push(r)
		s.downloadNotificationMu.Unlock()
	}else{
		ch = make(chan bool,2)

		s.downloadNotificationMu.Lock()
		s.downloadNotificationChanIdx++
		notifCHIdx := s.downloadNotificationChanIdx
		s.downloadNotificationChans[notifCHIdx] = ch
		s.downloadNotificationMu.Unlock()

		defer func() {
			s.downloadNotificationMu.Lock()
			delete(s.downloadNotificationChans,notifCHIdx)
			s.downloadNotificationMu.Unlock()
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}

		var replicatedCount uint

		s.downloadNoficationMu.RLock()
		for _, n := range s.downloadNotifications {
			if (n.chunk > chunkName) || (n.chunk == chunkName && n.size >= off) {
				replicatedCount++
			}
		}
		s.downloadNoficationMu.RUnlock()

		if replicatedCount >= minSyncReplicas {
			return nil
		}
	}
}

func (s *OnDisk) shortenLine(ln []byte) string {
	maxLen := 100
	if len(ln) <= maxLen {
		return string(ln)
	}
	return string(ln[:maxLen]) + "..."
}

func (s *OnDisk) getLastChunkFp() (*os.File, error) {
	if s.lastChunkFp != nil {
		return s.lastChunkFp, nil
	}

	fp, err := os.OpenFile(filepath.Join(s.dirname, s.lastChunk), os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("open chunk for writing: %v", err)
	}
	s.lastChunkFp = fp

	return fp, nil
}

func cutToLastMessage(res []byte) (truncated []byte, rest []byte, err error) {
	n := len(res)

	if n == 0 {
		return res, nil, nil
	}

	if res[n-1] == '\n' {
		return res, nil, nil
	}

	lastPos := bytes.LastIndexByte(res, '\n')
	if lastPos < 0 {
		return nil, nil, errBufTooSmall
	}
	return res[0 : lastPos+1], res[lastPos+1:], nil
}
