package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chukcha/protocol"
	"io"
	"log"
	"math/rand"
	"sort"
	"time"
)

const defaultSratchSize = 64 * 1024

// Simple 是一个连接到chukcha服务器的客户端实例
type Simple struct {
	Logger *log.Logger

	pollInterval time.Duration
	readTimeout  time.Duration

	acknowledge bool
	debug       bool
	addrs       []string
	cl          *Raw

	st *state
}

type state struct {
	Offsets map[string]*ReadOffset
}

type ReadOffset struct {
	CurChunk          protocol.Chunk
	LastAckedChunkIdx int
	Off               uint64
}

// 创建一个新的客户
func NewSimple() *Simple {
	return &Simple{}
}

// 无消息处理的时候，会间隔时间轮询kafka，这里设置该间隔时间
func (s *Simple) SetPollInterval(d time.Duration) {
	s.pollInterval = d
}

// 对读请求设置读超时
func (s *Simple) SetReadTimeout(v time.Duration) {
	s.readTimeout = v
}

// debug日志模式开关
func (s *Simple) SetDebug(v bool) {
	s.debug = v
	s.cl.SetDebug(v)
}

// TODO 这个具体是什么情况?
// acknowledge chunks
func (s *Simple) SetAcknowledge(v bool) {
	s.acknowledge = v
}

// TODO 这个具体是什么情况?
// 设置发送请求的最小异步副本数？
func (s *Simple) SetMinSyncReplicas(v uint) {
	s.cl.SetMinSyncReplicas(v)
}

// 编码本地的状态
func (s *Simple) MarshalState() ([]byte, error) {
	return json.Marshal(s.st)
}

// 读取状态
func (s *Simple) RestoreSavedState(buf []byte) error {
	return json.Unmarshal(buf, &s.st)
}

func (s *Simple) logger() *log.Logger {
	if s.Logger == nil {
		return log.Default()
	}
	return s.Logger
}

// 发消息
func (s *Simple) Send(ctx context.Context, category string, msgs []byte) error {
	//return
	return s.cl.Write(ctx, s.getAddr(), category, msgs)
}

var errRetry = errors.New("please retry the request")

// process要么等待新消息要么返回错误
// 仅在processFn返回no errors的情况下，read offset读偏移量才会继续向前偏移
func (s *Simple) Process(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultSratchSize)
	}

	for {
		err := s.tryProcess(ctx, category, scratch, processFn)
		if err == nil {
			return nil
		} else if errors.Is(err, errRetry) {
			continue
		} else if !errors.Is(err, io.EOF) {
			return err
		}

		select {
		case <-time.After(s.pollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Simple) tryProcess(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	addr := s.getAddr()

	if err := s.updateOffsets(ctx, category, addr); err != nil {
		return fmt.Errorf("updateCurrentChunk:%s", err)
	}

	needRetry := false

	for instance := range s.st.Offsets {
		err := s.processInstance(ctx, addr, instance, category, scratch, processFn)
		if errors.Is(err, io.EOF) {
			continue
		} else if errors.Is(err, errRetry) {
			needRetry = true
			continue
		}
	}
	if needRetry {
		return errRetry
	}
	return io.EOF
}

func (s *Simple) getAddr() string {
	addrIdx := rand.Intn(len(s.addrs))
	return s.addrs[addrIdx]
}

func (s *Simple) processInstance(parentCtx context.Context, addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	curChk := s.st.Offsets[instance]

	ctx, cancel := context.WithTimeout(parentCtx, s.readTimeout)
	defer cancel()

	res, found, err := s.cl.Read(ctx, addr, category, curChk.CurChunk.Name, curChk.Off, scratch)
	if err != nil {
		return err
	} else if !found {
		// TODO: if there are newer chunks than the one that we are processing,
		// update current chunk.
		// If the chunk that we are reading right now was acknowledged already,
		// then it we will never read newer chunks. This can happen if our state
		// is stale (e.g. client crashed and didn't save the last offset).
		if s.debug {

		}
		return io.EOF
	}

	if len(res) > 0 {
		err = processFn(res)
		if err == nil {
			curChk.Off += uint64(len(res))
		}
		return err
	}
	//按照惯例，读了0字节且没有报错，表示读完了
	if !curChk.CurChunk.Complete {
		if s.debug {

		}
		return io.EOF
	}
	nextChunk, err := s.getNextChunkForInstance(ctx, addr, instance, category, curChk.CurChunk.Name)
	if errors.Is(err, errNoNewChunks) {
		if s.debug {

		}
		return io.EOF
	} else if err != nil {
		return err
	}

	if s.acknowledge {
		if err := s.cl.Ack(ctx, addr, category, curChk.CurChunk.Name, curChk.Off); err != nil {
			return fmt.Errorf("ack current chunk: %w", err)
		}
	}

	if s.debug {

	}

	//更新状态
	_, idx := protocol.ParseChunkFileName(curChk.CurChunk.Name)
	curChk.LastAckedChunkIdx = idx
	curChk.CurChunk = nextChunk
	curChk.Off = 0

	if s.debug {

	}

	return errRetry
}

var errNoNewChunks = errors.New("no new chunks")

func (s *Simple) getNextChunkForInstance(ctx context.Context, addr, instance, category string, chunkName string) (protocol.Chunk, error) {
	_, idx := protocol.ParseChunkFileName(chunkName)
	lastAckedChunkIndexes := make(map[string]int)
	lastAckedChunkIndexes[instance] = idx
	chunksByInstance, err := s.getUnackedChunksGroupedByInstance(ctx, category, addr, lastAckedChunkIndexes)
	if err != nil {
		return protocol.Chunk{}, fmt.Errorf("getting chunks list before ack: unexpected error for instance %q: %w", instance, errNoNewChunks)
	}
	if len(chunksByInstance[instance]) == 0 {
		return protocol.Chunk{}, fmt.Errorf("%w", errNoNewChunks)
	}
	return chunksByInstance[instance][0], nil
}

func (s *Simple) getUnackedChunksGroupedByInstance(ctx context.Context, category, addr string, lastAckedChunkIndexes map[string]int) (map[string][]protocol.Chunk, error) {
	chunks, err := s.cl.ListChunks(ctx, addr, category, false)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	if len(chunks) == 0 {
		return nil, io.EOF
	}

	chunksByInstance := make(map[string][]protocol.Chunk)
	for _, c := range chunks {
		instance, chunkIdx := protocol.ParseChunkFileName(c.Name)
		if chunkIdx < 0 {
			continue
		}

		lastAckedChunkIdx, exists := lastAckedChunkIndexes[instance]
		if exists && chunkIdx <= lastAckedChunkIdx {
			continue
		}

		chunksByInstance[instance] = append(chunksByInstance[instance], c)
	}
	return chunksByInstance, nil
}

func (s *Simple) updateOffsets(ctx context.Context, category, addr string) error {
	lastAckedChunkIndexes := make(map[string]int, len(s.st.Offsets))
	for instance, off := range s.st.Offsets {
		lastAckedChunkIndexes[instance] = off.LastAckedChunkIdx
	}

	chunksByInstance, err := s.getUnackedChunksGroupedByInstance(ctx, category, addr, lastAckedChunkIndexes)
	if err != nil {
		return err
	}

	for instance, chunks := range chunksByInstance {
		off, exists := s.st.Offsets[instance]
		if exists {
			s.updateCurrentChunkInfo(chunks, off)
			continue
		}

		s.st.Offsets[instance] = &ReadOffset{
			CurChunk:          s.getOldestChunk(chunks),
			LastAckedChunkIdx: -1,
			Off:               0,
		}
	}
	return nil
}

func (s *Simple) getOldestChunk(chunks []protocol.Chunk) protocol.Chunk {
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Name < chunks[j].Name })

	for _, c := range chunks {
		if c.Complete {
			return c
		}
	}
	return chunks[0]
}

func (s *Simple) updateCurrentChunkInfo(chunks []protocol.Chunk, curCh *ReadOffset) {
	for _, c := range chunks {
		_, idx := protocol.ParseChunkFileName(c.Name)
		if idx < 0 {
			continue
		}

		if c.Name == curCh.CurChunk.Name {
			curCh.CurChunk = c
			return
		}
	}
}
