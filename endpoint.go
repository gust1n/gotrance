package zrpc

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	zmq "github.com/pebbe/zmq4"
)

const (
	EndpointReconnectInterval = time.Second * 5
)

// endpoint describes a service provider endpoint
type endpoint struct {
	address   string
	alive     bool
	connected chan struct{}
}

func (e *endpoint) Equal(other *endpoint) bool {
	return e.address == other.address
}

// endpoints describes a collection of endpoints
type endpoints struct {
	all []*endpoint

	// socket is a reference to the client in question to be able to handle its
	socket *zmq.Socket

	// Protects all endpoints
	sync.Mutex
}

// Add adds a new endpoint string to the endpoints collection
func (ends *endpoints) add(addr string) error {
	validAddr, err := validateAddress(addr)
	if err != nil {
		return err
	}

	// If already has it, there is no need to take any action
	if ends.has(validAddr) {
		return nil
	}

	// Setup a fresh endpoint
	ep := &endpoint{
		address:   validAddr,
		connected: make(chan struct{}),
	}

	ends.all = append(ends.all, ep)

	return ends.connect(ep)
}

func (ends *endpoints) connect(ep *endpoint) error {
	ends.Lock()
	defer ends.Unlock()

	glog.Infof("connecting to endpoint: '%s'", ep.address)

	// Make sure it's not alive
	ep.alive = false

	// The actual socket connect
	if err := ends.socket.Connect(ep.address); err != nil {
		return err
	}

	// Run in goroutine not to block the actual connecting process but write to passed channel for alive status
	go func() {
		select {
		case <-ep.connected:
			// When connected, we reset the connected chan
			ep.connected = make(chan struct{})
			// Mark as alive
			ep.alive = true
			glog.Info("zrpc: successfully connected to ", ep.address)
		}
	}()
	return nil
}

func (ends *endpoints) get(address string) (*endpoint, bool) {
	ends.Lock()
	defer ends.Unlock()

	for _, e := range ends.all {
		if e.address == address {
			return e, true
		}
	}

	return nil, false
}

// Del deletes an endpoint from the endpoints collection
func (ends *endpoints) del(addr string) error {
	validAddr, err := validateAddress(addr)
	if err != nil {
		return err
	}

	if !ends.has(validAddr) {
		return nil
	}

	ends.Lock()
	defer ends.Unlock()

	// The actual socket disconnect
	ends.socket.Disconnect(validAddr)

	// Remove from all endpoints
	for i, ep := range ends.all {
		if ep.address == validAddr {
			glog.Info("zrpc: removed endpoint: ", validAddr)
			ends.all = append(ends.all[:i], ends.all[i+1:]...)
		}
	}
	return nil
}

// Has checks for an endpoint in the endpoints collection
func (ends *endpoints) has(address string) bool {
	ends.Lock()
	defer ends.Unlock()

	for _, ep := range ends.all {
		if ep.address == address {
			return true
		}
	}
	return false
}

// Len returns the number of endpoints in the endpoints collection
func (ends *endpoints) len() int {
	ends.Lock()
	defer ends.Unlock()

	return len(ends.all)
}

func validateAddress(addr string) (string, error) {
	// Make sure it's a valid tcp address
	addr = strings.TrimPrefix(addr, "tcp://")
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s://%s", tcpAddr.Network(), tcpAddr.String()), nil
}
