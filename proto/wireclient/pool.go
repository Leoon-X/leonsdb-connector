// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package wireclient

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Pool is a set of connections to a LeonsDB server, each used by one call
// at a time: a call takes a free connection, sends its request and reads
// the answer itself. For callers that wait for each answer, as most Go
// code does, that costs the least: on a Client, which many calls share,
// another goroutine reads the answers and hands each one over, a wake-up
// more per call. A Client suits callers that keep many calls in flight
// without waiting, and streams.
//
// A Pool implements grpc.ClientConnInterface for unary calls, like a
// Client:
//
//	p, err := wireclient.NewPool(ctx, "db.example:7422", token, 64, wireclient.Options{})
//	data := leonsdbv1.NewDataServiceClient(p)
//
// Connections are opened as calls need them, up to size; one that fails is
// closed, and a later call opens another.
type Pool struct {
	addr, token string
	opts        Options
	// InstanceID identifies the database, as in ServerService.GetInfo.
	InstanceID string

	free  chan *poolConn
	slots chan struct{} // one per connection open or being opened

	mu     sync.Mutex
	closed bool
	open   map[*poolConn]struct{}
}

type poolConn struct {
	nc      net.Conn
	r       *bufio.Reader
	methods map[string]uint16
	nextID  uint32
}

// NewPool opens the first of up to size connections, to check the address
// and token.
func NewPool(ctx context.Context, addr, token string, size int, opts Options) (*Pool, error) {
	if size < 1 {
		return nil, errors.New("wireclient: a pool needs at least one connection")
	}
	p := &Pool{
		addr: addr, token: token, opts: opts,
		free: make(chan *poolConn, size), slots: make(chan struct{}, size), open: map[*poolConn]struct{}{},
	}
	p.slots <- struct{}{}
	c, err := p.dial(ctx)
	if err != nil {
		return nil, err
	}
	p.free <- c
	return p, nil
}

// dial opens a connection, in a slot already taken.
func (p *Pool) dial(ctx context.Context) (*poolConn, error) {
	nc, r, w, err := handshake(ctx, p.addr, p.token, p.opts)
	if err != nil {
		<-p.slots
		return nil, err
	}
	c := &poolConn{nc: nc, r: r, methods: make(map[string]uint16, len(w.Methods))}
	for i, m := range w.Methods {
		c.methods[m] = uint16(i)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		nc.Close()
		<-p.slots
		return nil, ErrClosed
	}
	p.InstanceID = w.InstanceId
	p.open[c] = struct{}{}
	return c, nil
}

// get returns a free connection, opening one while there are fewer than
// size, else waiting for one.
func (p *Pool) get(ctx context.Context) (*poolConn, error) {
	select {
	case c := <-p.free:
		return c, nil
	default:
	}
	select {
	case c := <-p.free:
		return c, nil
	case p.slots <- struct{}{}:
		return p.dial(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// put gives c back, or closes it when it broke.
func (p *Pool) put(c *poolConn, broken bool) {
	p.mu.Lock()
	closed := p.closed
	if broken || closed {
		delete(p.open, c)
	}
	p.mu.Unlock()
	if broken || closed {
		c.nc.Close()
		<-p.slots
		return
	}
	p.free <- c
}

// cutCall stops a call's reading and writing once its context ended. It
// runs on a goroutine of its own, maybe late; tests replace it to make it
// late on purpose.
var cutCall = func(nc net.Conn) { _ = nc.SetDeadline(time.Now()) }

// Invoke makes a unary call. It implements grpc.ClientConnInterface.
func (p *Pool) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	// A context that already ended fails the call before it is sent:
	// otherwise a fast answer could beat the deadline call sets.
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	timeout, err := requestTimeout(ctx)
	if err != nil {
		return err
	}
	c, err := p.get(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		return status.Error(codes.Unavailable, err.Error())
	}
	broken, err := c.call(ctx, method, timeout, args, reply, opts)
	p.put(c, broken)
	return err
}

// call sends one request and reads its answer. broken is true when the
// connection cannot be used again.
func (c *poolConn) call(ctx context.Context, method string, timeout uint32, args, reply any, opts []grpc.CallOption) (broken bool, err error) {
	m, ok := c.methods[method]
	if !ok {
		return false, status.Errorf(codes.Unimplemented, "%s is not available over the wire protocol", method)
	}
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	id := c.nextID
	req, err := encodeRequest(id, m, timeout, args)
	if err != nil {
		return false, err
	}
	// The call may not outlast its context: when it ends, reading and
	// writing stop at once, and the connection is given up.
	if dl, ok := ctx.Deadline(); ok {
		_ = c.nc.SetDeadline(dl)
		defer func() { _ = c.nc.SetDeadline(time.Time{}) }()
	}
	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { cutCall(c.nc) })
		defer func() {
			if !stop() {
				broken = true // its deadline was moved, or is about to be
			}
		}()
	}
	fail := func(e error) (bool, error) {
		if ctx.Err() != nil {
			return true, status.FromContextError(ctx.Err()).Err()
		}
		return true, status.Error(codes.Unavailable, fmt.Sprintf("wireclient: %v", e))
	}
	if _, err := c.nc.Write(req); err != nil {
		return fail(err)
	}
	typ, body, err := readFrame(c.r)
	if err != nil {
		return fail(err)
	}
	if (typ != frameResponse && typ != frameError) || len(body) < 4 || binary.BigEndian.Uint32(body) != id {
		return fail(fmt.Errorf("unexpected frame %d", typ))
	}
	return false, decode(result{body: body[4:], typ: typ}, reply, opts)
}

// NewStream implements grpc.ClientConnInterface; streams need a Client.
func (p *Pool) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "streams need a wireclient.Client or gRPC")
}

// Close closes the connections; calls in flight fail.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	for c := range p.open {
		c.nc.Close()
	}
	return nil
}
