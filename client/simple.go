package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
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
	//CurChunk protocol.C
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

func (s *Simple) Process(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultSratchSize)
	}

	for {

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

}

func (s *Simple) updateOffsets(ctx context.Context, category, addr string) error {

}
