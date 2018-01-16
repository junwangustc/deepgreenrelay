package proxy

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type testWriter struct {
}

func (t *testWriter) write(buf []byte, query string) (*responseData, error) {
	return &responseData{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		StatusCode:      http.StatusOK,
		Body:            []byte("passed"),
	}, nil
}

func newTestWriter() *testWriter {
	return &testWriter{}
}
func getHTTPInstance() (*HTTP, error) {
	outputs := make([]HTTPOutputConfig, 3)
	for i := 1; i < 4; i++ {
		name := "test_judge" + strconv.Itoa(i)
		location := "http://0.0.0.0:" + strconv.Itoa(i*1000)
		o := HTTPOutputConfig{
			Name:     name,
			Location: location,
		}
		outputs = append(outputs, o)
	}
	h, err := NewHTTP(HTTPConfig{
		Addr:    "0.0.0.0:10086",
		Name:    "test",
		Outputs: outputs,
		Time:    500,
	})
	if err != nil {
		return nil, err
	}

	for _, b := range h.backends {
		b.writer = newTestWriter()
	}
	return h, nil
}

func TestProxy_ServeHTTP(t *testing.T) {
	//create request
	req, err := http.NewRequest("POST", "/write", strings.NewReader(
		"cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000",
	))

	if err != nil {
		t.Fatal(err)
	}

	//get HTTP instance for testing
	h, _ := getHTTPInstance()

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(h.ServeHTTP)

	req.Header.Set("Content-Type", "")

	params := req.URL.Query()
	params.Set("db", "foo")
	params.Set("rp", "")
	params.Set("precision", "")
	params.Set("consistency", "")
	req.URL.RawQuery = params.Encode()

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.

	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusNoContent {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusNoContent)
	}

	// Check the response body is what we expect.
	expected := ""
	if rr.Body.String() != expected {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func Concurrent_Use_Server(handler http.HandlerFunc, t *testing.T) *httptest.ResponseRecorder {
	//create request
	req, err := http.NewRequest("POST", "/write", strings.NewReader(
		"cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000",
	))

	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()

	req.Header.Set("Content-Type", "")

	params := req.URL.Query()
	params.Set("db", "foo")
	params.Set("rp", "")
	params.Set("precision", "")
	params.Set("consistency", "")
	req.URL.RawQuery = params.Encode()

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.

	handler.ServeHTTP(rr, req)

	return rr
}

//TODO: have to make points much more complicated
func TestProxy_Concurrent_Use(t *testing.T) {
	//get HTTP instance for testing
	h, _ := getHTTPInstance()
	handler := http.HandlerFunc(h.ServeHTTP)
	n := 3
	time := 3
	var wg sync.WaitGroup
	wg.Add(time)
	recoders := make(chan *httptest.ResponseRecorder, 3*n)
	for i := 0; i < time; i++ {
		go func() {
			defer wg.Done()
			rr := Concurrent_Use_Server(handler, t)
			recoders <- rr
		}()
	}

	go func() {
		wg.Wait()
		close(recoders)
	}()

	for rr := range recoders {
		// Check the status code is what we expect.
		if status := rr.Code; status != http.StatusNoContent {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusNoContent)
		}

		// Check the response body is what we expect.
		expected := ""
		if rr.Body.String() != expected {
			t.Errorf("handler returned unexpected body: got %v want %v",
				rr.Body.String(), expected)
		}

	}
}
