// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

// Package wireclient is a Go client for the LeonsDB wire protocol, a lean
// binary protocol over TCP for the calls a Minecraft server makes most
// (docs/wire-protocol.md).
//
// A Client implements grpc.ClientConnInterface for unary calls, so the
// generated clients work over it unchanged:
//
//	c, err := wireclient.Dial(ctx, "db.example:7422", token, wireclient.Options{})
//	data := leonsdbv1.NewDataServiceClient(c)
//
// Errors are gRPC status errors, as over gRPC; trailers (the error reason)
// are delivered to grpc.Trailer call options. Many calls may be in flight
// at once on one Client; requests that are ready together are sent in one
// write. Streams of answers to one request (subscriptions) work over it
// too; streams of requests need gRPC. A Pool gives each call a connection
// of its own instead, which costs less for callers that wait for each
// answer.
package wireclient

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	leonsdbv1 "github.com/Leoon-X/leonsdb-connector/proto/gen/go/leonsdb/v1"
)

// ProtocolVersion is the wire protocol version this client speaks.
const ProtocolVersion = 1

const (
	frameHello    = 1
	frameWelcome  = 2
	frameRequest  = 3
	frameResponse = 4
	frameError    = 5
	frameMessage  = 6
	frameHeader   = 7
	frameCancel   = 8

	maxFrame = 16<<20 + 64
)

// Options configures Dial.
type Options struct {
	// TLS, when set, is used for the connection; it must match the server.
	TLS *tls.Config
	// ClientName appears in the server's logs.
	ClientName string
}

// Client is one connection to a LeonsDB server. It is safe for concurrent
// use.
type Client struct {
	nc      net.Conn
	methods map[string]uint16
	// InstanceID identifies the database, as in ServerService.GetInfo.
	InstanceID string

	out *combiner

	mu      sync.Mutex
	nextID  uint32
	pending map[uint32]chan result
	streams map[uint32]*clientStream
	err     error // set when the connection is broken
	done    chan struct{}
}

type result struct {
	body []byte
	typ  byte
}

// ErrClosed is returned by calls on a closed or broken client.
var ErrClosed = errors.New("wireclient: connection closed")

// handshake connects, authenticates with token and returns the connection,
// its reader and the server's welcome.
func handshake(ctx context.Context, addr, token string, opts Options) (net.Conn, *bufio.Reader, *leonsdbv1.WireWelcome, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, nil, err
	}
	if opts.TLS != nil {
		tc := tls.Client(nc, opts.TLS)
		if err := tc.HandshakeContext(ctx); err != nil {
			nc.Close()
			return nil, nil, nil, err
		}
		nc = tc
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(dl)
	}
	r := bufio.NewReaderSize(nc, 64<<10)
	hello, _ := proto.Marshal(&leonsdbv1.WireHello{ProtocolVersion: ProtocolVersion, Token: token, ClientName: opts.ClientName})
	if _, err := nc.Write(frame(frameHello, nil, hello)); err != nil {
		nc.Close()
		return nil, nil, nil, err
	}
	typ, body, err := readFrame(r)
	if err != nil {
		nc.Close()
		return nil, nil, nil, err
	}
	switch typ {
	case frameError:
		nc.Close()
		var we leonsdbv1.WireError
		_ = proto.Unmarshal(body[min(4, len(body)):], &we)
		return nil, nil, nil, status.Error(codes.Code(we.Code), we.Message)
	case frameWelcome:
	default:
		nc.Close()
		return nil, nil, nil, fmt.Errorf("wireclient: unexpected frame %d", typ)
	}
	var w leonsdbv1.WireWelcome
	if err := proto.Unmarshal(body, &w); err != nil {
		nc.Close()
		return nil, nil, nil, err
	}
	_ = nc.SetDeadline(time.Time{})
	return nc, r, &w, nil
}

// Dial connects and authenticates with token.
func Dial(ctx context.Context, addr, token string, opts Options) (*Client, error) {
	nc, r, w, err := handshake(ctx, addr, token, opts)
	if err != nil {
		return nil, err
	}
	c := &Client{
		nc: nc, methods: make(map[string]uint16, len(w.Methods)), InstanceID: w.InstanceId,
		pending: map[uint32]chan result{}, streams: map[uint32]*clientStream{}, done: make(chan struct{}),
	}
	c.out = &combiner{w: nc, onFail: func(err error) { c.fail(fmt.Errorf("wireclient: write: %w", err)) }}
	for i, m := range w.Methods {
		c.methods[m] = uint16(i)
	}
	go c.read(r)
	return c, nil
}

// Invoke makes a unary call. It implements grpc.ClientConnInterface.
func (c *Client) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	m, ok := c.methods[method]
	if !ok {
		return status.Errorf(codes.Unimplemented, "%s is not available over the wire protocol", method)
	}
	timeout, err := requestTimeout(ctx)
	if err != nil {
		return err
	}

	ch := make(chan result, 1)
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return status.Error(codes.Unavailable, c.err.Error())
	}
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	id := c.nextID
	c.pending[id] = ch
	c.mu.Unlock()

	req, err := encodeRequest(id, m, timeout, args)
	if err != nil {
		c.forget(id)
		return err
	}
	c.out.send(req)

	select {
	case res := <-ch:
		return decode(res, reply, opts)
	case <-c.done:
		// A response may have arrived just before the connection broke.
		select {
		case res := <-ch:
			return decode(res, reply, opts)
		default:
		}
		c.forget(id)
		return status.Error(codes.Unavailable, c.brokenErr().Error())
	case <-ctx.Done():
		c.forget(id)
		return status.FromContextError(ctx.Err()).Err()
	}
}

func decode(res result, reply any, opts []grpc.CallOption) error {
	if res.typ == frameError {
		var we leonsdbv1.WireError
		if err := proto.Unmarshal(res.body, &we); err != nil {
			return status.Errorf(codes.Internal, "wireclient: malformed error: %v", err)
		}
		if len(we.Trailers) > 0 {
			md := metadata.New(we.Trailers)
			for _, o := range opts {
				if t, ok := o.(grpc.TrailerCallOption); ok && t.TrailerAddr != nil {
					*t.TrailerAddr = md
				}
			}
		}
		return status.Error(codes.Code(we.Code), we.Message)
	}
	msg, ok := reply.(proto.Message)
	if !ok {
		return status.Error(codes.Internal, "wireclient: reply is not a protobuf message")
	}
	if err := proto.Unmarshal(res.body, msg); err != nil {
		return status.Errorf(codes.Internal, "wireclient: malformed response: %v", err)
	}
	return nil
}

// NewStream implements grpc.ClientConnInterface for streams of answers to
// one request, such as subscriptions (see stream.go); streams of requests
// need gRPC.
func (c *Client) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.newStream(ctx, desc, method, opts)
}

// Close closes the connection; calls in flight fail.
func (c *Client) Close() error {
	c.fail(ErrClosed)
	return nil
}

func (c *Client) forget(id uint32) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Client) brokenErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *Client) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	c.err = err
	close(c.done)
	c.nc.Close()
}

func (c *Client) read(r *bufio.Reader) {
	for {
		typ, body, err := readFrame(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errors.New("the server closed the connection")
			}
			c.fail(fmt.Errorf("wireclient: %w", err))
			return
		}
		if typ < frameResponse || typ > frameHeader || len(body) < 4 {
			c.fail(fmt.Errorf("wireclient: unexpected frame %d", typ))
			return
		}
		id := binary.BigEndian.Uint32(body)
		c.mu.Lock()
		if s := c.streams[id]; s != nil {
			if typ == frameResponse || typ == frameError {
				delete(c.streams, id)
			}
			c.mu.Unlock()
			s.deliver(typ, body[4:])
			continue
		}
		ch := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if ch != nil {
			ch <- result{body: body[4:], typ: typ}
		}
	}
}

func readFrame(r *bufio.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:4])
	if n < 1 || n > maxFrame {
		return 0, nil, fmt.Errorf("frame of %d bytes", n)
	}
	body := make([]byte, n-1)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return hdr[4], body, nil
}

func frameStart(typ byte, body []byte) []byte {
	b := make([]byte, 5, 5+len(body)+64)
	b[4] = typ
	return append(b, body...)
}

func finish(f []byte) []byte {
	binary.BigEndian.PutUint32(f, uint32(len(f)-4))
	return f
}

func frame(typ byte, hdr, msg []byte) []byte {
	return finish(append(frameStart(typ, hdr), msg...))
}

// requestTimeout is what a request tells the server of its deadline, in
// milliseconds (0: none).
func requestTimeout(ctx context.Context) (uint32, error) {
	dl, ok := ctx.Deadline()
	if !ok {
		return 0, nil
	}
	d := time.Until(dl)
	if d <= 0 {
		return 0, status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error())
	}
	return uint32(min(d.Milliseconds()+1, 1<<31)), nil
}

// encodeRequest makes the frame of a request.
func encodeRequest(id uint32, m uint16, timeout uint32, args any) ([]byte, error) {
	req, ok := args.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "wireclient: request is not a protobuf message")
	}
	var hdr [10]byte
	binary.BigEndian.PutUint32(hdr[:], id)
	binary.BigEndian.PutUint16(hdr[4:], m)
	binary.BigEndian.PutUint32(hdr[6:], timeout)
	payload, err := proto.MarshalOptions{}.MarshalAppend(frameStart(frameRequest, hdr[:]), req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "wireclient: encode request: %v", err)
	}
	return finish(payload), nil
}
