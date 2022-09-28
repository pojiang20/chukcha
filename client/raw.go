package client

import (
	"bytes"
	"context"
	"fmt"
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
