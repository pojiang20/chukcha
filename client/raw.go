package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/chukcha/protocol"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

// Raw是http客户端，能使用服务器的api发送请求
// 可以在它的基础上，构建自己的客户端
type Raw struct {
	Logger *log.Logger

	minSyncReplicas uint
	debug           bool
	cl              *http.Client
}

func NewRaw(cl *http.Client) *Raw {
	if cl == nil {
		cl = &http.Client{}
	}
	return &Raw{
		cl: cl,
	}
}

func (r *Raw) SetDebug(v bool) {
	r.debug = v
}

func (r *Raw) SetMinSyncReplicas(v uint) {
	r.minSyncReplicas = v
}

func (r *Raw) logger() *log.Logger {
	if r.Logger == nil {
		log.Default()
	}
	return r.Logger
}

// 发送事件给适合的服务
func (r *Raw) Write(ctx context.Context, addr string, category string, msgs []byte) (err error) {
	u := url.Values{}
	u.Add("category", category)
	if r.minSyncReplicas > 0 {
		u.Add("min_sync_replicas", strconv.FormatUint(uint64(r.minSyncReplicas), 10))
	}

	url := addr + "/write?" + u.Encode()

	if r.debug {

	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(msgs))
	if err != nil {
		fmt.Errorf("make http request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := r.cl.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http status %s,%s", resp.Status, b.String())
	}

	//TODO 为什么需要这一步？
	io.Copy(io.Discard, resp.Body)
	return nil
}

// scratch是干啥的
func (r *Raw) Read(ctx context.Context, addr string, category string, chunk string, off uint64, scratch []byte) (res []byte, found bool, err error) {
	u := url.Values{}
	u.Add("off", strconv.FormatInt(int64(off), 10))
	u.Add("max_size", strconv.Itoa(len(scratch)))
	u.Add("chunk", chunk)
	u.Add("category", category)

	readUrl := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	if r.debug {

	}

	req, err := http.NewRequestWithContext(ctx, "GET", readUrl, nil)
	if err != nil {
		return nil, false, fmt.Errorf("creating Request:%v", err)
	}
	resp, err := r.cl.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("read %q:%v", readUrl, err)
	}
	defer resp.Body.Close()

	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("read %q:%v", readUrl, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	} else if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("GET %q: http status %s,%s", readUrl, resp.Status, b.String())
	}

	return b.Bytes(), true, nil
}

func (r *Raw) ListChunks(ctx context.Context, addr, category string, fromReplication bool) ([]protocol.Chunk, error) {
	u := url.Values{}
	u.Add("category", category)
	if fromReplication {
		u.Add("from_replication", "1")
	}

	listURL := fmt.Sprintf("%s/listChunks?%s", addr, u.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", listURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating new http request: %w", err)
	}

	resp, err := r.cl.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("http status %s: %s", resp.Status, body)
	}

	var res []protocol.Chunk
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	if r.debug {

	}

	return res, nil
}

func (r *Raw) ListCategories(ctx context.Context, addr string) ([]string, error) {
	listURL := fmt.Sprintf("%s/listtCategories", addr)

	req, err := http.NewRequestWithContext(ctx, "GET", listURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating new http request: %w", err)
	}

	resp, err := r.cl.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("listCategories error: %s", body)
	}

	var res []string
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	if r.debug {

	}
	return res, nil
}

func (r *Raw) Ack(ctx context.Context, addr, category, chunk string, size uint64) error {
	u := url.Values{}
	u.Add("chunk", chunk)
	u.Add("size", strconv.FormatInt(int64(size), 10))
	u.Add("category", category)

	if r.debug {

	}
	listURL := fmt.Sprintf(addr+"/ack?%s", u.Encode())
	req, err := http.NewRequestWithContext(ctx, "GET", listURL, nil)
	if err != nil {
		return fmt.Errorf("creating new http request: %w", err)
	}

	resp, err := r.cl.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("http status %s, %s", resp.Status, body)
	}

	//TODO 检测是否有必要
	//io.Copy(io.Discard,resp.Body)
	return nil
}
