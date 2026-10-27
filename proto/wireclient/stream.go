// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package wireclient

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A stream of answers to one request, such as a subscription to channels:
// the request goes out as a request frame, the server sends a header frame
// once the stream has begun (as gRPC's response headers), then one message
// frame per answer, and ends it with a response frame (empty) or an error
// frame. Ending it early sends a cancel frame.

// streamBuffer is how many messages a stream holds that were not received
// yet; beyond that, reading the connection waits for the stream's receiver.
const streamBuffer = 4096

type clientStream struct {
	c      *Client
	id     uint32
	method uint16
	ctx    context.Context
	opts   []grpc.CallOption
	stop   func() bool // of the context's AfterFunc

	items  chan result
	header chan struct{} // closed when the stream has begun or ended

	mu         sync.Mutex
	sent       bool
	headerDone bool
	ended      error   // io.EOF, or the error that ended it
	refused    *result // the error frame that ended it before it began
	trailer    metadata.MD
}

func (c *Client) newStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts []grpc.CallOption) (grpc.ClientStream, error) {
	if desc.ClientStreams {
		return nil, status.Error(codes.Unimplemented, "streams of requests need gRPC")
	}
	m, ok := c.methods[method]
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, "%s is not available over the wire protocol", method)
	}
	s := &clientStream{c: c, method: m, ctx: ctx, opts: opts, items: make(chan result, streamBuffer), header: make(chan struct{})}
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return nil, status.Error(codes.Unavailable, c.err.Error())
	}
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	s.id = c.nextID
	c.streams[s.id] = s
	c.mu.Unlock()
	s.stop = context.AfterFunc(ctx, s.cancel)
	return s, nil
}

// deliver hands the stream a frame the reader read for it.
func (s *clientStream) deliver(typ byte, body []byte) {
	switch typ {
	case frameHeader:
		s.beginOnce()
	case frameMessage:
		s.beginOnce()
		s.items <- result{body: body, typ: typ}
	default: // the end
		s.mu.Lock()
		if !s.headerDone && typ == frameError {
			s.refused = &result{body: body, typ: typ}
		}
		s.mu.Unlock()
		s.items <- result{body: body, typ: typ}
		s.beginOnce()
	}
}

func (s *clientStream) beginOnce() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.headerDone {
		s.headerDone = true
		close(s.header)
	}
}

// cancel ends the stream when its context ends.
func (s *clientStream) cancel() {
	s.c.mu.Lock()
	_, running := s.c.streams[s.id]
	delete(s.c.streams, s.id)
	s.c.mu.Unlock()
	if !running {
		return
	}
	var f [9]byte
	f[4] = frameCancel
	f[5], f[6], f[7], f[8] = byte(s.id>>24), byte(s.id>>16), byte(s.id>>8), byte(s.id)
	s.c.out.send(finish(f[:]))
	s.finish(status.FromContextError(s.ctx.Err()).Err())
	s.beginOnce()
}

// finish records how the stream ended, the first time.
func (s *clientStream) finish(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended == nil {
		s.ended = err
		if s.stop != nil {
			s.stop()
		}
	}
	return s.ended
}

// Header waits until the stream has begun, and fails if the server refused
// it instead.
func (s *clientStream) Header() (metadata.MD, error) {
	select {
	case <-s.header:
	case <-s.c.done:
		return nil, status.Error(codes.Unavailable, s.c.brokenErr().Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refused != nil {
		return nil, decode(*s.refused, nil, s.opts)
	}
	if s.ended != nil && !errors.Is(s.ended, io.EOF) {
		return nil, s.ended
	}
	return metadata.MD{}, nil
}

func (s *clientStream) Trailer() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trailer
}

func (s *clientStream) CloseSend() error         { return nil }
func (s *clientStream) Context() context.Context { return s.ctx }

// SendMsg sends the stream's one request.
func (s *clientStream) SendMsg(m any) error {
	s.mu.Lock()
	if s.sent {
		s.mu.Unlock()
		return status.Error(codes.Internal, "wireclient: a stream of answers has one request")
	}
	s.sent = true
	s.mu.Unlock()
	timeout, err := requestTimeout(s.ctx)
	if err != nil {
		return err
	}
	req, err := encodeRequest(s.id, s.method, timeout, m)
	if err != nil {
		return err
	}
	s.c.out.send(req)
	return nil
}

// RecvMsg receives the next answer; io.EOF when the stream is over.
func (s *clientStream) RecvMsg(m any) error {
	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if ended != nil && len(s.items) == 0 {
		return ended
	}
	var r result
	select {
	case r = <-s.items:
	case <-s.c.done:
		select {
		case r = <-s.items:
		default:
			return s.finish(status.Error(codes.Unavailable, s.c.brokenErr().Error()))
		}
	case <-s.ctx.Done():
		s.cancel()
		return s.finish(status.FromContextError(s.ctx.Err()).Err())
	}
	switch r.typ {
	case frameMessage:
		msg, ok := m.(proto.Message)
		if !ok {
			return status.Error(codes.Internal, "wireclient: reply is not a protobuf message")
		}
		if err := proto.Unmarshal(r.body, msg); err != nil {
			return status.Errorf(codes.Internal, "wireclient: malformed message: %v", err)
		}
		return nil
	case frameError:
		var md metadata.MD
		err := decode(r, nil, append(s.opts, grpc.Trailer(&md)))
		s.mu.Lock()
		s.trailer = md
		s.mu.Unlock()
		return s.finish(err)
	default:
		return s.finish(io.EOF)
	}
}
