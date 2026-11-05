package xsocket

import (
	"sync"
	"time"
)

const (
	StatusNew      = 0
	StatusSend     = 1
	StatusResponse = 2
	DefaultTTL     = 3000
)

type Data struct {
	EventEmitter
	mu           sync.Mutex
	isOutput     bool
	socketObject *SocketObject
	dataID       string
	dataType     string
	data         any
	ttl          int
	time         int64
	resData      any
	error        string
	status       int
	destroy      string
	ttlTimer     *time.Timer
	doneCh       chan struct{}
}

func newData(isOutput bool, ID string, dataType string, data any, ttl any, dataTime any, socketObject *SocketObject) *Data {
	d := &Data{
		isOutput:     isOutput,
		socketObject: socketObject,
		dataID:       ID,
		dataType:     dataType,
		data:         data,
		resData:      map[string]any{},
		status:       StatusNew,
		doneCh:       make(chan struct{}),
	}
	d.SetMaxListeners(0)
	if !isObject(data) {
		d.data = map[string]any{}
	}
	d.ttl = DefaultTTL
	if f, ok := toFloat(ttl); ok && isInt(ttl) && f >= 0 {
		d.ttl = int(f)
	}
	var now int64
	if socketObject != nil {
		now = socketObject.GetTime()
	} else {
		now = getTime()
	}
	d.time = now
	if f, ok := toFloat(dataTime); ok && f > 0 {
		d.time = int64(f)
	}
	if d.ttl > 0 {
		delay := min(int64(d.ttl), int64(d.ttl)-(now-d.time))
		d.mu.Lock()
		d.ttlTimer = time.AfterFunc(time.Duration(max(0, delay))*time.Millisecond, func() {
			d.destroyData("ttl", true)
		})
		d.mu.Unlock()
	}
	return d
}

func (d *Data) GetID() string {
	return d.dataID
}

func (d *Data) GetType() string {
	return d.dataType
}

func (d *Data) GetTTL() int {
	return d.ttl
}

func (d *Data) GetTime() int64 {
	return d.time
}

func (d *Data) GetData() any {
	return d.data
}

func (d *Data) GetDataVal(key any, def ...any) any {
	return getVal(d.GetData(), key, def)
}

func (d *Data) GetResData() any {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.resData
}

func (d *Data) GetResDataVal(key any, def ...any) any {
	return getVal(d.GetResData(), key, def)
}

func (d *Data) GetStatus() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.status
}

func (d *Data) GetDestroy() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.destroy
}

func (d *Data) GetSocketObject() *SocketObject {
	return d.socketObject
}

func (d *Data) GetError() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.error
}

func (d *Data) IsNew() bool {
	return d.GetStatus() == StatusNew
}

func (d *Data) IsSend() bool {
	return d.GetStatus() == StatusSend
}

func (d *Data) IsResponse() bool {
	return d.GetStatus() == StatusResponse
}

func (d *Data) Done() <-chan struct{} {
	return d.doneCh
}

func (d *Data) Response(resData any, errMsg ...string) (*Data, error) {
	if d.isOutput {
		return d, newError("Output data")
	}
	d.mu.Lock()
	isDestroy := d.destroy != "" || d.status == StatusResponse || d.ttl == 0
	d.mu.Unlock()
	if isDestroy {
		return d, newError("destroy")
	}
	d.SetResData(resData)
	if len(errMsg) > 0 {
		d.SetError(errMsg[0])
	}
	ws, err := d.socketObject.getWsReady(d.doneCh)
	if err != nil {
		d.destroyData(err.Error(), false)
	} else if ws.SendObject([]any{msgSDUpdate, toInt(d.dataID, 0), d.GetResData(), d.getErrorValue()}) {
		d.SetStatus(StatusResponse)
	} else {
		d.destroyData("Send failed", false)
	}
	<-d.doneCh
	if msg := d.GetDestroy(); msg != "response" {
		return d, newError(msg)
	}
	return d, nil
}

func (d *Data) getErrorValue() any {
	if errMsg := d.GetError(); errMsg != "" {
		return errMsg
	}
	return false
}

func (d *Data) SetStatus(status int) bool {
	d.mu.Lock()
	if (status != StatusSend && status != StatusResponse) || d.status == StatusResponse || d.status == status {
		d.mu.Unlock()
		return false
	}
	d.status = status
	d.mu.Unlock()
	d.Emit("setStatus", status)
	if status == StatusResponse {
		d.Emit("response", d.GetResData(), d.GetError())
		d.destroyData("response", false)
	}
	return true
}

func (d *Data) SetError(errMsg string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.error = errMsg
	return true
}

func (d *Data) SetResData(resData any) bool {
	if resData == nil {
		resData = map[string]any{}
	}
	if !isObject(resData) {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resData = resData
	return true
}

func (d *Data) Destroy(msg string) bool {
	return d.destroyData(msg, false)
}

func (d *Data) destroyData(msg string, isSetError bool) bool {
	d.mu.Lock()
	if d.destroy != "" {
		d.mu.Unlock()
		return false
	}
	if msg == "" {
		msg = "destroy"
	}
	d.destroy = msg
	if d.ttlTimer != nil {
		d.ttlTimer.Stop()
	}
	if isSetError && d.error == "" {
		d.error = msg
	}
	isResponse := d.status == StatusResponse
	d.mu.Unlock()
	if !isResponse {
		d.SetStatus(StatusResponse)
	}
	d.Emit("destroy", msg)
	close(d.doneCh)
	d.ClearAllListeners()
	return true
}
