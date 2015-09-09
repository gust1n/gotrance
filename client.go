package zrpc

import (
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/gust1n/zrpc/util"
	"github.com/gust1n/zrpc/watch"

	"code.google.com/p/go.net/context"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	zmq "github.com/pebbe/zmq4"
	"github.com/satori/go.uuid"
)

const (
	RequestTimeout = time.Second * 2
	RouterURL      = "inproc://router"
)

var (
	ErrShutdown           = errors.New("zrpc: connection is shut down")
	ErrMessageMismatch    = errors.New("zrpc: received message ID did not match the one sent")
	ErrTimeout            = errors.New("zrpc: request timeout")
	ErrCancel             = errors.New("zrpc: request cancelled")
	ErrServiceNameMissing = errors.New("zrpc: missing service name (hint: pass it through the context)")

	errDone = errors.New("zrpc: call done")
)

// Client is used to interact with a service
type Client struct {
	conn   *zmq.Socket
	router *zmq.Socket
	ctx    *zmq.Context

	endpoints endpoints

	mutex    sync.Mutex // protects following
	closing  bool       // user has called Close
	shutdown bool       // server has told us to stop
}

// Call represents an active call to a service.
type Call struct {
	Service string           // The service to send to
	Req     proto.Message    // The request to the function.
	Resp    proto.Message    // The reply from the function.
	cancelc chan interface{} // Cancels an ongoing request
}

// NewClient returns a new Client to handle requests to the
// service at the other end of the connection.
// A watcher is used to get updates about service endpoints
func NewClient(serviceName string, watcher watch.Watcher) *Client {
	// Dial with no addresses means adding no endpoints
	client, err := Dial()
	if err != nil {
		glog.Fatal(err)
	}

	// Handle updates about service endpoints
	serviceChannel := make(chan watch.ServiceUpdate)
	go util.Forever(func() {
		serviceUpdate := <-serviceChannel
		switch serviceUpdate.Op {
		case watch.ADD:
			if err := client.endpoints.add(serviceUpdate.Value); err != nil {
				glog.Error(err)
			}
		case watch.REMOVE:
			if err := client.endpoints.del(serviceUpdate.Value); err != nil {
				glog.Error(err)
			}
		default:
			glog.Warning("zrpc: unknown service update op")
		}
	}, time.Second)

	// Register as a listener for passed service name
	watcher.Watch(serviceName, serviceChannel)

	return client
}

// NewClientWithConnection returns a new Client to handle requests to the
// set of services at the other end of the connection.
// An existing connection in the form of a zmq socket together with the zmq
// context they were created with is used
func NewClientWithConnection(ctx *zmq.Context, conn *zmq.Socket) *Client {
	// A router socket is the middle-man between client requests and actually sending/receiving on the wire
	router, err := ctx.NewSocket(zmq.ROUTER)
	if err != nil {
		glog.Fatal(err)
	}
	if err := router.Bind(RouterURL); err != nil {
		glog.Fatal(err)
	}

	client := &Client{
		conn: conn,
		endpoints: endpoints{
			socket: conn,
		},
		router: router,
		ctx:    ctx,
	}

	// Start the proxy in an own routine since it is blocking
	go func() {
		if err := zmq.Proxy(conn, router, nil); err != nil {
			switch zmq.AsErrno(err) {
			case zmq.Errno(zmq.ETERM):
				glog.Info(err)
			case zmq.Errno(syscall.EINTR):
				glog.Info(err)
			default:
				glog.Info(zmq.AsErrno(err))
				glog.Info(err)
			}
			client.Close()
		}
	}()

	// Socket monitor for connect event
	monitorURL := "inproc://monitor"
	if err := conn.Monitor(monitorURL, zmq.EVENT_CONNECTED|zmq.EVENT_DISCONNECTED); err != nil {
		client.Close()
		glog.Fatal(err)
	}
	go client.monitor(monitorURL)

	return client
}

// Dial connects to a zrpc server at the specified network address
// Protocol is limited to tcp
func Dial(address ...string) (*Client, error) {
	// Don't use the global context to avoid package level confusion
	ctx, err := zmq.NewContext()
	if err != nil {
		glog.Fatal(err)
	}
	// A dealer socket handles the actual connection
	socket, err := ctx.NewSocket(zmq.DEALER)
	if err != nil {
		glog.Fatal(err)
	}

	client := NewClientWithConnection(ctx, socket)

	for _, addr := range address {
		if err := client.endpoints.add(addr); err != nil {
			return nil, err
		}
	}

	return client, nil
}

func (client *Client) send(ctx context.Context, call *Call) error {
	if client.shutdown || client.closing {
		return ErrShutdown
	}

	// Every call gets its own req socket
	sock, err := client.ctx.NewSocket(zmq.REQ)
	if err != nil {
		return err
	}
	defer sock.Close()

	// Connect it to the router
	if err := sock.Connect(RouterURL); err != nil {
		return err
	}

	// Marshal the outgoing message
	msgBytes, err := proto.Marshal(call.Req)
	if err != nil {
		return err
	}

	// Envelope the message
	reqID := uuid.NewV4()
	envelope := &Request{
		UUID:    reqID.Bytes(),
		Path:    proto.String(fmt.Sprintf("zrpc://%s/%s", call.Service, util.GetMessageName(call.Req))),
		Payload: msgBytes,
	}

	// If request has a timeout, send it in the request envelope
	d, ok := ctx.Deadline()
	if ok {
		envelope.Expires = proto.Int64(d.Unix())
	}

	// Marshal the outgoing envelope
	envBytes, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}

	// Send it
	if _, err := sock.SendBytes(envBytes, 0); err != nil {
		return err
	}

	handleResponse := func(state zmq.State) error {
		respBytes, err := sock.RecvBytes(0)
		if err != nil {
			return err
		}

		// Unmarshal the response envelope
		resp := &Response{}
		if err := proto.Unmarshal(respBytes, resp); err != nil {
			return err
		}

		// Make sure the same message ID was received
		respID, err := uuid.FromBytes(resp.UUID)
		if err != nil || !uuid.Equal(reqID, respID) {
			glog.Errorf("Mismatching message IDs, sent '%s', got '%s'", reqID, respID)
			return ErrMessageMismatch
		}

		// Check if there is an error
		if resp.Error != nil {
			return NewClientError(resp.Error.GetMessage(), int(resp.GetStatusCode()))
		}

		// Decode the actual message (if exists and wanted)
		if call.Resp != nil && len(resp.Payload) > 0 {
			if err := proto.Unmarshal(resp.Payload, call.Resp); err != nil {
				return err
			}
		}
		// This is insane, but the reactor runs until an error is returned...
		return errDone
	}

	// Use a reactor to be able to utilize the calls cancelc
	reactor := zmq.NewReactor()
	reactor.AddSocket(sock, zmq.POLLIN, handleResponse)
	reactor.AddChannel(call.cancelc, 1, func(interface{}) error {
		return ErrCancel
	})
	// Poll for a short interval to be able to return to the channel handling
	if err := reactor.Run(time.Millisecond * 50); err != errDone {
		return err
	}
	return nil
}

func (client *Client) Close() error {
	glog.Info("zrpc: client closing")
	client.mutex.Lock()
	if client.closing {
		client.mutex.Unlock()
		return ErrShutdown
	}
	client.closing = true
	client.mutex.Unlock()

	// These should block until all messages returned
	client.conn.Close()
	client.router.Close()

	// Terminate the zmq context
	return zmq.Term()
}

func (client *Client) monitor(addr string) {
	s, err := client.ctx.NewSocket(zmq.PAIR)
	if err != nil {
		glog.Fatal(err)
	}
	defer s.Close()

	if err := s.Connect(addr); err != nil {
		glog.Fatal(err)
	}

	for {
		evtType, addr, _, err := s.RecvEvent(0)
		if err != nil {
			switch zmq.AsErrno(err) {
			case zmq.Errno(zmq.ETERM):
				glog.Info(err)
			case zmq.Errno(syscall.EINTR):
				glog.Info(err)
			default:
				glog.Error(err)
			}
			break
		}

		// Only care about events for our added endpoints
		if end, ok := client.endpoints.get(addr); ok {
			switch evtType {
			case zmq.EVENT_CONNECTED:
				// Flag endpoint as connected
				if !end.alive {
					close(end.connected)
				} else {
					glog.Info("zrpc: reconnected to ", end.address)
				}
			case zmq.EVENT_DISCONNECTED:
				glog.Infof("zrpc: %s disconnected ", end.address)
			}
		}
	}
}

// Call sends the request, waits for it to complete, and returns its error status.
func (client *Client) Call(ctx context.Context, req proto.Message, resp proto.Message) error {
	// Setup a new call
	call := &Call{
		Req:     req,
		Resp:    resp,
		cancelc: make(chan interface{}),
	}

	// Make sure a service name is passed
	if serviceName, ok := ServiceNameFromContext(ctx); !ok {
		return ErrServiceNameMissing
	} else {
		call.Service = serviceName
	}

	// If a manual deadline is not passed, setup with default timeout
	if _, ok := ctx.Deadline(); !ok {
		ctx, _ = context.WithDeadline(ctx, time.Now().Add(RequestTimeout))
	}

	// Run the request in a goroutine and write the reponse to c
	c := make(chan error, 1)
	go func() { c <- client.send(ctx, call) }()
	select {
	// Use context done to manually trigger cancel
	case <-ctx.Done():
		// Cancel the request in flight
		call.cancelc <- true
		<-c // Wait for the request to finish
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ctx.Err()
	// Request finished
	case err := <-c:
		return err
	}
}
