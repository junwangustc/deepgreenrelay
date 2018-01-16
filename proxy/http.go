package proxy

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"encoding/json"

	"github.com/influxdata/influxdb/models"
)

type HTTP struct {
	addr         string
	name         string
	schema       string
	closing      int64
	l            net.Listener
	buf          map[string]*bytes.Buffer
	times        int
	total        map[string]int
	timeRWLock   sync.RWMutex
	bufRWLock    sync.RWMutex
	filterMetric map[string]int
	bufferLen    int
	bufferTimes  int
	goNum        int
	Ttimes       int
	DispelMetric []Dispel
}

const (
	DefaultHTTPTimeout      = 10 * time.Second
	DefaultMaxDelayInterval = 10 * time.Second
	DefaultBatchSizeKB      = 512

	KB = 1024
	MB = 1024 * KB
)

func NewHTTP(config HTTPConfig) (*HTTP, error) {
	log.Println("config is ", config)
	h := new(HTTP)

	h.addr = config.Addr
	h.name = config.Name
	h.Ttimes = config.Times
	h.DispelMetric = config.DispelMetric

	h.schema = "http"
	h.buf = make(map[string]*bytes.Buffer)
	h.filterMetric = make(map[string]int)
	h.bufferLen = config.BufferLen
	h.bufferTimes = config.BufferTimes
	h.goNum = config.GoNum
	for _, v := range config.FilterMetric {
		//		name := h.NormalizeMetricName(v)
		h.filterMetric[v] = 1
	}
	h.total = make(map[string]int)
	return h, nil
}

func (h *HTTP) Name() string {
	if h.name == "" {
		return fmt.Sprintf("%s://%s", h.schema, h.addr)
	}

	return h.name
}

func (h *HTTP) Run() error {
	l, err := net.Listen("tcp", h.addr)
	if err != nil {
		return err
	}
	h.l = l
	go h.GetData()

	log.Printf("Starting %s proxy %q on %v", strings.ToUpper(h.schema), h.Name(), h.addr)

	err = http.Serve(l, h)
	if atomic.LoadInt64(&h.closing) != 0 {
		return nil
	}
	return err
}

func (h *HTTP) Reload() error {
	return nil
}

func (h *HTTP) Stop() error {
	atomic.StoreInt64(&h.closing, 1)
	return h.l.Close()
}
func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/write" {
		return
	}

	start := time.Now()
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			responseError(w, Error{http.StatusMethodNotAllowed, "invalid write method"})
		}
		return
	}

	queryParams := r.URL.Query()
	database := queryParams.Get("db")
	if database == "" {
		responseError(w, Error{http.StatusBadRequest, "missing parameter: db"})
		return
	}

	body := r.Body

	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			responseError(w, Error{http.StatusBadRequest, "failed to read body as gzip format"})
			return
		}
		defer b.Close()
		body = b
	}

	bodyBuf := getBuf()
	_, err := bodyBuf.ReadFrom(body)
	if err != nil {
		putBuf(bodyBuf)
		responseError(w, Error{http.StatusInternalServerError, "problem reading request body"})
		return
	}

	precision := r.URL.Query().Get("precision")
	points, parseError := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	if parseError != nil {
		responseError(w, Error{http.StatusBadRequest, "unable to parse points"})
		return
	}

	h.bufRWLock.Lock()
	for _, pt := range points {
		ptName := pt.Name()
		if _, ok := h.filterMetric[ptName]; !ok {
			continue
		}
		res := false
		for _, dis := range h.DispelMetric {
			tags := pt.Tags()
			_, ok := tags[dis.TagKey]
			if dis.MetricName == ptName && ok && tags[dis.TagKey] == dis.TagValue {
				res = true
				break
			}
		}
		if res {
			continue
		}
		if _, ok := h.buf[ptName]; !ok {
			h.buf[ptName] = getBuf()
			h.buf[ptName].WriteString(pt.PrecisionString(precision))
			h.buf[ptName].WriteByte('\n')
		} else {
			h.buf[ptName].WriteString(pt.PrecisionString(precision))
			h.buf[ptName].WriteByte('\n')
		}
	}

	h.times++
	for name, _ := range h.buf {
		if _, ok := h.total[name]; !ok {
			h.total[name] = 1
		} else {
			h.total[name] = h.total[name] + 1
		}

	}
	if h.times >= h.bufferTimes {
		log.Println("h.times values is ", h.times, "buffer_times is ", h.bufferTimes)
		tmpMap := make(map[string]*bytes.Buffer)
		for name, buf := range h.buf {
			if buf.Len() >= h.bufferLen {
				tmpMap[name] = h.buf[name]
				delete(h.buf, name)
				delete(h.total, name)
				log.Println("字节数达到要求", h.bufferLen*1.0/1000000, "M", name)
			}
		}
		for name, _ := range h.buf {
			log.Println("name is ", name, " current times is  ", h.total[name])
			if h.total[name] >= h.Ttimes*h.bufferTimes {
				log.Println("时间达到 bufferTimes", h.bufferTimes*h.Ttimes, name)
				tmpMap[name] = h.buf[name]
				delete(h.buf, name)
				delete(h.total, name)
				break
			}
		}

		go h.ProcessPoints(tmpMap)
		h.times = 0
	}
	h.bufRWLock.Unlock()
	putBuf(bodyBuf)
}

var byteBufChan = make(chan *bytes.Buffer, 200000)

func (h *HTTP) MultiProcess(pts models.Points) *bytes.Buffer {
	buf := getBuf()
	ptlen := len(pts) - 1
	if ptlen > 3000 {
		var n = h.goNum
		var bufChan = make(chan *bytes.Buffer, n)
		for i := 0; i < n; i++ {

			start := i * ptlen / n
			end := (i + 1) * ptlen / n

			go h.GorutineProcess(pts[start:end], bufChan)

		}
		for i := 0; i < n; i++ {
			data, ok := <-bufChan
			if ok {

				buf.Write(data.Bytes())
				putBuf(data)
			}

		}
		buf.Write([]byte(pt2str(pts[ptlen])))

	} else {
		for i := 0; i < ptlen; i++ {
			buf.Write([]byte(pt2str(pts[i])))
			buf.Write([]byte("\n"))
		}
		if ptlen >= 0 {
			buf.Write([]byte(pt2str(pts[ptlen])))
		}

	}
	return buf
}
func (h *HTTP) GorutineProcess(pts models.Points, data chan *bytes.Buffer) {
	buf := getBuf()
	for _, pt := range pts {
		buf.Write([]byte(pt2str(pt)))
		buf.Write([]byte("\n"))
	}
	data <- buf

}
func (h *HTTP) NormalizeMetricName(name string) string {

	lowerName := strings.ToLower(name)
	return strings.Replace(lowerName, ".", "_", -1)

}
func (h *HTTP) ProcessPoints(bufMap map[string]*bytes.Buffer) {
	for k, _ := range bufMap {
		//normalName := h.NormalizeMetricName(k)
		normalName := k
		if _, ok := h.filterMetric[normalName]; ok {
			byteBufChan <- bufMap[k]
		} else {
			putBuf(bufMap[k])
		}
	}

}

func (h *HTTP) GetData() {
	for {
		buf, ok := <-byteBufChan
		log.Println("Channel 中size 为", len(byteBufChan))
		if ok {
			h.InsertData(buf)
		}
	}
}
func (h *HTTP) InsertData(buf *bytes.Buffer) {
	pts, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now(), "")
	if parseError != nil {
		log.Println("PARSE ERROR")
		putBuf(buf)
		return
	}

	metricName := h.NormalizeMetricName(pts[0].Name())
	var exitChan = make(chan int, 1)
	go h.RunGPLoad(metricName, buf, exitChan)
	h.gploadFile(metricName, pts)
	data := <-exitChan
	if data == 2 {
		log.Println("ERROR 不正常的超时退出")
	}
	log.Println("InsertDataOver")

}
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
func getCurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	return strings.Replace(dir, "\\", "/", -1)
}
func (h *HTTP) gploadFile(tableName string, pts []models.Point) {
	res, err := pathExists(getCurrentDirectory() + "/" + tableName)
	if err != nil {
		log.Println("PathExist error", err)
		return
	}
	if !res {
		_, err := exec.Command("mkfifo", tableName).Output() ///查看当前目录下文件
		if err != nil {
			log.Println(err)
		}
	}
	ptlen := len(pts)
	log.Println("聚合", tableName, ptlen, "个点")
	buf := h.MultiProcess(pts)

	f, err1 := os.OpenFile(tableName, os.O_WRONLY, os.ModeNamedPipe)
	if err1 != nil {
		log.Println(err1)
		return
	}
	len, err := f.WriteString(buf.String())
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("write ", len/1000, " k bytes ")
	f.Close()
}

const loaddata = `---
VERSION: 1.0.0.1
DATABASE: esm
USER: gpadmin
HOST: 10.0.38.59
PORT: 5434
PASSWORD: gpadmin
GPLOAD:
  INPUT:
      - SOURCE:
          FILE:
            - /home/gpadmin/load_data/tableName
      - COLUMNS:
            - metric_name: varchar(255)
            - tags: json
            - fields: json
            - time: timestamp
      - FORMAT: text
      - DELIMITER: ','
      - ERROR_LIMIT: 25
  OUTPUT:
      - TABLE: tableName
      - MODE: INSERT
  SQL:
      - BEFORE: ""
      - AFTER: ""
`
const createTable = `CREATE TABLE tableName(metric_name varchar(255),tags json,fields json,time timestamp) WITH (appendonly=true,orientation=column,compresstype=zlib,COMPRESSLEVEL=5) distributed by (metric_name);`

func (h *HTTP) RunGPLoad(tableName string, buf *bytes.Buffer, exit chan int) {
	ymldata := strings.Replace(loaddata, "tableName", tableName, -1)
	fileName := "/home/gpadmin/load_data/" + tableName + ".yml"
	_, err := os.Stat(fileName)
	if err != nil {
		log.Println("Create Yml ", fileName)
		f, err1 := os.Create(fileName)
		if err1 != nil {
			log.Println("Create YML File Error ", err)
		}
		f.WriteString(ymldata)
		// no such file or dir
		f.Close()
	}
	//在这里执行创建表
	sqldata := strings.Replace(createTable, "tableName", tableName, -1)
	fileNameTable := "/home/gpadmin/load_data/create" + tableName + ".sql"
	_, err = os.Stat(fileNameTable)
	if err != nil {
		log.Println("Create sql ", fileNameTable)
		f, err1 := os.Create(fileNameTable)
		if err1 != nil {
			log.Println("Create SQL File Error ", err)
		}
		f.WriteString(sqldata)
		f.Close()

		cmd := exec.Command("psql", "-d", "esm", "-p", "5434", "-f", fileNameTable)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Println(err)
			exit <- 1
			putBuf(buf)
			return
		}
		// 保证关闭输出流
		defer stdout.Close()
		// 运行命令
		if err := cmd.Start(); err != nil {
			log.Println(err)
			putBuf(buf)
			exit <- 1
			return
		}
		// 读取输出结果
		opBytes, err := ioutil.ReadAll(stdout)
		if err != nil {
			log.Println(err)
			putBuf(buf)
			exit <- 1
			return
		}
		log.Println(string(opBytes))
	}
	cmd := exec.Command("gpload", "-f", fileName)
	over := make(chan int, 1)
	go h.GPLoad(cmd, fileName, tableName, over)
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(8 * time.Second) //等待1秒钟
		timeout <- true
	}()
	for {
		select {
		case data := <-over:
			putBuf(buf)
			exit <- data
			return
		case <-timeout:
			log.Println("ERROR GPLOAD ", tableName, " time out ")
			err := cmd.Process.Kill()
			if err != nil {
				log.Println("Kill Process Error", err)
				putBuf(buf)
				exit <- 2
				return
			}
		}
	}

}

func (h *HTTP) GPLoad(cmd *exec.Cmd, fileName, tableName string, over chan int) {
	startTime := time.Now()
	// 获取输出对象，可以从该对象中读取输出结果
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Println(err)
		return
	}
	// 保证关闭输出流
	defer stdout.Close()
	// 运行命令
	if err := cmd.Start(); err != nil {
		log.Println(err)
		return
	}
	// 读取输出结果
	opBytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(string(opBytes))
	log.Println("插入 Spend", tableName, time.Now().Sub(startTime).Seconds())
	if time.Now().Sub(startTime).Seconds() > 7 {
		log.Println("Kill The Process")
		over <- 2
	} else {
		over <- 1
	}
}
func pt2str(pt models.Point) string {
	name := pt.Tags()["host"]
	fields := pt.Fields()
	tags := pt.Tags()
	time := pt.Time().Format("2006-01-02 15:04:05")
	var res = name + "," + map2str(tags) + "," + map2str(fields) + "," + time
	return res
}

func map2str(dataMap interface{}) string {
	data, _ := json.Marshal(dataMap)
	tmp := strings.Replace(string(data), "\"", "\\\"", -1)
	res := strings.Replace(string(tmp), ",", "\\,", -1)
	return res
}

var ErrBufferFull = errors.New("retry buffer full")

var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

func getBuf() *bytes.Buffer {
	if bb, ok := bufPool.Get().(*bytes.Buffer); ok {
		return bb
	}
	return new(bytes.Buffer)
}

func putBuf(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}

type httpBackend struct {
	writer
	name     string
	location string
}

/*
func newHTTPBackend(config *HTTPOutputConfig) (*httpBackend, error) {
	if config.Name == "" {
		config.Name = config.Location
	}
	timeout := DefaultHTTPTimeout
	if config.Timeout != "" {
		t, err := time.ParseDuration(config.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout'%v", err)
		}
		timeout = t
	}

	var w writer = newSimpleWriter(config.Location, timeout)

	// If configured, create a retryBuffer per backend.
	// This way we serialize retries against each backend.
	if config.BufferSizeMB > 0 {
		max := DefaultMaxDelayInterval
		if config.MaxDelayInterval != "" {
			m, err := time.ParseDuration(config.MaxDelayInterval)
			if err != nil {
				return nil, fmt.Errorf("error parsing max retry time %v", err)
			}
			max = m
		}

		batch := DefaultBatchSizeKB * KB
		if config.MaxBatchKB > 0 {
			batch = config.MaxBatchKB * KB
		}

		w = newRetryBuffer(config.BufferSizeMB*MB, batch, max, w)
	}

	return &httpBackend{
		writer:   w,
		name:     config.Name,
		location: config.Location,
	}, nil
}
*/
type writer interface {
	write([]byte, string) (*responseData, error)
}

type simpleWriter struct {
	client   *http.Client
	location string
}

func newSimpleWriter(location string, timeout time.Duration) *simpleWriter {
	return &simpleWriter{
		client: &http.Client{
			Timeout: timeout,
		},
		location: location,
	}
}

func (s *simpleWriter) Location() string {
	return s.location
}
func (s *simpleWriter) write(buf []byte, query string) (*responseData, error) {
	req, err := http.NewRequest("POST", s.location, bytes.NewReader(buf))
	if err != nil {
		return &responseData{
			ContentType:     "",
			ContentEncoding: "",
			StatusCode:      http.StatusServiceUnavailable,
			Body:            []byte("not available"),
		}, err
	}

	req.URL.RawQuery = query
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))

	resp, err := s.client.Do(req)

	if err != nil {
		return &responseData{
			ContentType:     "",
			ContentEncoding: "",
			StatusCode:      http.StatusServiceUnavailable,
			Body:            []byte("not available"),
		}, err
	}

	rd := &responseData{
		ContentType:     resp.Header.Get("Conent-Type"),
		ContentEncoding: resp.Header.Get("Conent-Encoding"),
		StatusCode:      resp.StatusCode,
		Body:            []byte("not available"),
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return rd, err
	}

	rd.Body = data

	if err = resp.Body.Close(); err != nil {
		return rd, err
	}

	return rd, nil
}

type responseData struct {
	ContentType     string
	ContentEncoding string
	StatusCode      int
	Body            []byte
}

func (rd *responseData) Write(w http.ResponseWriter) {
	if rd.ContentType != "" {
		w.Header().Set("Content-Type", rd.ContentType)
	}

	if rd.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", rd.ContentEncoding)
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(rd.Body)))
	w.WriteHeader(rd.StatusCode)
	w.Write(rd.Body)
}

type Error struct {
	code int
	Err  string
}
type JsonHTTP struct {
	TotalMeasurement int
	TotalField       int
	Backends         []JsonBackend
}

type JsonBackend struct {
	Name             string
	Location         string
	TotalMeasurement int
	Measurements     []string
	TotalField       int
}

//responseError will call responseJson. Additionally, it provides a extra Error info which
//can be used to build json
func responseError(w http.ResponseWriter, e Error) {
	responseJson(w, e.code, e)
}

//responseJson will build json and reply to caller.
func responseJson(w http.ResponseWriter, code int, v interface{}) {
	bytes, err := json.Marshal(v)
	if err != nil {
		log.Printf("marshal response %v: %v", v, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(bytes)))
	w.WriteHeader(code)
	if v != nil {
		if _, err := w.Write(bytes); err != nil {
			log.Printf("write response %v: %v", bytes, err)
			return
		}
	}
	return
}
