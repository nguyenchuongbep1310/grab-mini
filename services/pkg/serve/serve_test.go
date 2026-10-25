package serve

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestRunStopsCleanlyWhenTheContextIsCancelled(t *testing.T) {
	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, addr, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }))
	}()

	var resp *http.Response
	var err error
	for i := 0; i < 50; i++ { // wait for the listener
		if resp, err = http.Get("http://" + addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Errorf("Run = %v after cancel, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

func TestRunReportsAListenError(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	if err := Run(context.Background(), l.Addr().String(), http.NotFoundHandler()); err == nil {
		t.Error("Run on a port that is already taken returned nil")
	}
}

func TestServerSetsTimeoutsAndHeaderLimit(t *testing.T) {
	srv := Server(":0", http.NotFoundHandler())
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 || srv.MaxHeaderBytes <= 0 {
		t.Errorf("a limit is unset: %+v", srv)
	}
}

func TestAClientThatNeverFinishesItsHeadersIsCutOff(t *testing.T) {
	addr := freeAddr(t)
	srv := Server(addr, http.NotFoundHandler())
	srv.ReadHeaderTimeout = 150 * time.Millisecond
	go srv.ListenAndServe()
	defer srv.Close()

	var c net.Conn
	var err error
	for i := 0; i < 50; i++ {
		if c, err = net.Dial("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n")) // never sends the blank line that ends the headers
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadAll(c); err != nil {
		t.Fatalf("the server kept a half-sent request open: %v", err)
	}
}

func TestLimitBodyStopsAHandlerReadingMoreThanTheLimit(t *testing.T) {
	var readErr error
	h := LimitBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, readErr = io.Copy(io.Discard, r.Body)
	}), 10)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader("0123456789")))
	if readErr != nil {
		t.Errorf("a body at the limit failed: %v", readErr)
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader("0123456789x")))
	var tooBig *http.MaxBytesError
	if !errors.As(readErr, &tooBig) {
		t.Errorf("a body over the limit gave %v, want *http.MaxBytesError", readErr)
	}
}
