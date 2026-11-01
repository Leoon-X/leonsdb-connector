// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package wireclient

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	leonsdbv1 "github.com/Leoon-X/leonsdb-connector/proto/gen/go/leonsdb/v1"
)

// fastPool is a pool of one connection to a fake server that answers every
// request at once with an empty response. It counts the requests it got.
func fastPool(t *testing.T) (*Pool, *atomic.Int32) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	var got atomic.Int32
	go func() {
		r := bufio.NewReader(server)
		for {
			typ, body, err := readFrame(r)
			if err != nil {
				return
			}
			if typ != frameRequest || len(body) < 4 {
				continue
			}
			got.Add(1)
			if _, err := server.Write(frame(frameResponse, body[:4], nil)); err != nil {
				return
			}
		}
	}()
	c := &poolConn{nc: client, r: bufio.NewReader(client), methods: map[string]uint16{"/leonsdb.v1.DataService/Get": 0}}
	p := &Pool{free: make(chan *poolConn, 1), slots: make(chan struct{}, 1), open: map[*poolConn]struct{}{c: {}}}
	p.slots <- struct{}{}
	p.free <- c
	return p, &got
}

func TestThePoolWorks(t *testing.T) {
	p, got := fastPool(t)
	if err := p.Invoke(context.Background(), "/leonsdb.v1.DataService/Get", &leonsdbv1.GetRequest{}, &leonsdbv1.GetResponse{}); err != nil {
		t.Fatal(err)
	}
	if got.Load() != 1 {
		t.Fatalf("the server got %d requests, want 1", got.Load())
	}
}

// A call whose context ended before it began fails, and is never sent,
// even when the server answers faster than the context's end can stop the
// call. That order is made certain here: the stop comes only after the
// call returned, the latest it can.
func TestACallWithAnEndedContextIsNotSent(t *testing.T) {
	returned := make(chan struct{})
	cut := cutCall
	cutCall = func(nc net.Conn) {
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
		}
		cut(nc)
	}
	t.Cleanup(func() { cutCall = cut })

	p, got := fastPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.Invoke(ctx, "/leonsdb.v1.DataService/Get", &leonsdbv1.GetRequest{}, &leonsdbv1.GetResponse{})
	close(returned)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("a call with an ended context: %v, want Canceled", err)
	}
	if got.Load() != 0 {
		t.Fatalf("the server got %d requests from a call whose context had ended", got.Load())
	}
}
