package zrpc

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/gust1n/zrpc/util"
	zmq "github.com/pebbe/zmq4"
)

// Precompute the reflect type for error.  Can't use error directly
// because Typeof takes an empty interface value.  This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

type methodType struct {
	sync.Mutex // protects counters
	method     reflect.Method
	ArgType    reflect.Type
	ReplyType  reflect.Type
	numCalls   uint
}

type service struct {
	name   string                 // name of service
	rcvr   reflect.Value          // receiver of methods for the service
	typ    reflect.Type           // type of the receiver
	method map[string]*methodType // registered methods
}

// Server represents an RPC Server.
type Server struct {
	conn       *zmq.Socket  // the remote connection
	closing    bool         // user has called close
	mu         sync.RWMutex // protects the serviceMap
	serviceMap map[string]*service
	numWorkers int
}

// NewServer returns a new Server.
func NewServer(workers int) *Server {
	return &Server{
		numWorkers: workers,
		serviceMap: make(map[string]*service),
	}
}

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//      - exported method
//      - two arguments, both of exported type
//      - the second argument is a pointer
//      - one return value, of type error
// It returns an error if the receiver is not an exported type or has
// no suitable methods. It also logs the error using package log.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
func (server *Server) Register(rcvr interface{}) error {
	return server.register(rcvr, "", false)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (server *Server) RegisterName(name string, rcvr interface{}) error {
	return server.register(rcvr, name, true)
}

// Start registers a zmq endpoint at passed address answering requests for registered services
func (server *Server) Start(addr string) {
	// Don't use the global context to avoid package level confusion
	ctx, err := zmq.NewContext()
	if err != nil {
		glog.Fatal(err)
	}
	// A router socket handles the actual connection
	sock, _ := ctx.NewSocket(zmq.ROUTER)
	server.conn = sock

	// If no prefix is passed, default to tcp
	if !strings.HasPrefix(addr, "tcp://") {
		addr = "tcp://" + addr
	}
	server.conn.Bind(addr)
	glog.Info("Server listening on ", addr)

	// Socket monitor
	monitorURL := "inproc://monitor"
	if err := server.conn.Monitor(monitorURL, zmq.EVENT_ACCEPTED|zmq.EVENT_DISCONNECTED); err != nil {
		glog.Fatal(err)
	}
	go server.monitor(ctx, monitorURL)

	// A dealer socket multiplexes requests to workers
	mux, _ := ctx.NewSocket(zmq.DEALER)
	defer mux.Close()
	mux.Bind("inproc://mux")

	// Start backing worker processes
	for i := 0; i < server.numWorkers; i++ {
		go func(i int) {
			worker, _ := ctx.NewSocket(zmq.REP)
			defer worker.Close()
			worker.Connect("inproc://mux")
			glog.V(2).Infof("Started worker #%d", i)

			for {
				if server.closing {
					glog.Warning(ErrShutdown)
					break
				}
				reqBytes, err := worker.RecvBytes(0)
				if err != nil {
					switch zmq.AsErrno(err) {
					// If was interrupted there is no need to log as an error
					case zmq.Errno(zmq.ETERM):
						glog.Info(err)
					default:
						// Error receiving is usually fatal
						glog.Error(err)
					}
					break
				}

				// Decode the request envelope
				req := &Request{}
				if err := proto.Unmarshal(reqBytes, req); err != nil {
					glog.Error(err)
					sendError(worker, nil, err)
					continue
				}

				// Make sure it's not expired on arrival
				if req.Expires != nil {
					if time.Unix(*req.Expires, 0).Before(time.Now()) {
						glog.Infof("discarding expired message: '%s'", req.UUID)
						sendError(worker, req, NewExpiredError("message expired on arrival"))
						continue
					}
				}

				serviceName := path.Dir(strings.TrimPrefix(req.GetPath(), "zrpc://"))
				methodName := path.Base(req.GetPath())

				// Make sure a handler for this request exists
				server.mu.RLock()
				service, ok := server.serviceMap[serviceName]
				server.mu.RUnlock()
				if !ok {
					err := fmt.Sprintf("service '%s' is not served", serviceName)
					if serviceName == "." {
						err = "no service name passed"
					}
					glog.Warning(err)
					sendError(worker, req, errors.New(err))
					continue
				}

				// Make sure the message is registered for this server
				if mType, ok := service.method[methodName]; ok {
					// Decode the incoming request message
					var argv reflect.Value
					argIsValue := false // if true, need to indirect before calling.
					if mType.ArgType.Kind() == reflect.Ptr {
						argv = reflect.New(mType.ArgType.Elem())
					} else {
						argv = reflect.New(mType.ArgType)
						argIsValue = true
					}

					// argv guaranteed to be a pointer now.
					if err := proto.Unmarshal(req.Payload, argv.Interface().(proto.Message)); err != nil {
						glog.Error(err)
						sendError(worker, req, err)
						continue
					}

					if argIsValue {
						argv = reflect.Indirect(argv)
					}

					glog.V(3).Infof("Received '%s' (%s)", argv.Type().Elem(), req.UUID)

					// Invoke the method, providing a new value for the reply (if expected)
					var (
						returnValues []reflect.Value
						replyv       reflect.Value
					)
					if mType.ReplyType != nil {
						replyv = reflect.New(mType.ReplyType.Elem())
						returnValues = mType.method.Func.Call([]reflect.Value{service.rcvr, argv, replyv})
					} else {
						returnValues = mType.method.Func.Call([]reflect.Value{service.rcvr, argv})
					}
					// The return value for the method is an error.
					errInter := returnValues[0].Interface()
					if errInter != nil {
						err := errInter.(error)
						sendError(worker, req, err)
						continue
					}

					// Envelope the response message
					envelope := &Response{
						Path: req.Path,
						UUID: req.UUID,
					}

					// Marshal the response message (if exists)
					if mType.ReplyType != nil {
						replyBytes, err := proto.Marshal(replyv.Interface().(proto.Message))
						if err != nil {
							glog.Error(err)
							sendError(worker, req, err)
							continue
						}
						envelope.Payload = replyBytes
					}

					// Marshal the envelope
					envBytes, err := proto.Marshal(envelope)
					if err != nil {
						glog.Error(err)
						sendError(worker, req, err)
						continue
					}

					// Send the response
					if _, err := worker.SendBytes(envBytes, 0); err != nil {
						// Since we could not send, we could not send an error either, just log
						glog.Error(err)
					}
					if mType.ReplyType != nil {
						glog.V(3).Infof("Replied '%s' (%s)", mType.ReplyType.Elem(), envelope.UUID)
					} else {
						glog.V(3).Infof("Replied nil (%s)", envelope.UUID)
					}
				} else {
					// If reached here, the message was not handled by the server
					glog.V(1).Infof("message '%s' is not handled by this service", methodName)
					sendError(worker, req, fmt.Errorf("message '%s' is not handled by this service", methodName))
				}
			}

			glog.Infof("Closing worker #%d", i)
		}(i + 1)
	}

	// This is blocking so we put it last
	if err := zmq.Proxy(sock, mux, nil); err != nil {
		switch zmq.AsErrno(err) {
		// If was interrupted there is no need to log as an error
		case zmq.Errno(syscall.EINTR):
			glog.Info(err)
		case zmq.Errno(zmq.ETERM):
			glog.Info(err)
		default:
			glog.Error(err)
		}
	}

	// Since it was blocking we could safely close the server if reached here
	server.Close()
}

// Close closes the server, usually deferred upon starting it
func (server *Server) Close() {
	// Don't do anything is already closing
	if server.closing {
		return
	}
	server.closing = true
	glog.Info("zrpc: server closing...")
	// Should block until all requests are done
	server.conn.Close()
	// Terminates the entire zmq context
	zmq.Term()
}

func sendError(socket *zmq.Socket, req *Request, err error) {
	// Response envelope
	resp := &Response{
		Error: &Response_Error{},
	}

	if req != nil {
		resp.UUID = req.UUID
	}

	// If error is a zrpc error
	if zrpcErr, ok := err.(zrpcError); ok {
		resp.StatusCode = proto.Uint32(uint32(zrpcErr.GetStatusCode()))
		resp.Error.Message = proto.String(zrpcErr.GetMessage())
	} else {
		// Default to internal error
		resp.StatusCode = proto.Uint32(uint32(http.StatusInternalServerError))
		resp.Error.Message = proto.String(err.Error())
	}

	// Encode the response
	buf, protoErr := proto.Marshal(resp)
	if protoErr != nil {
		glog.Error(protoErr)
		return
	}

	// Send the response
	if _, err := socket.SendBytes(buf, 0); err != nil {
		glog.Error(err)
	}
}

func (server *Server) register(rcvr interface{}, name string, useName bool) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.serviceMap == nil {
		server.serviceMap = make(map[string]*service)
	}
	s := new(service)
	s.typ = reflect.TypeOf(rcvr)
	s.rcvr = reflect.ValueOf(rcvr)
	sname := reflect.Indirect(s.rcvr).Type().Name()
	if useName {
		sname = name
	}
	if sname == "" {
		s := "zrpc.Register: no service name for type " + s.typ.String()
		glog.Error(s)
		return errors.New(s)
	}
	if !isExported(sname) && !useName {
		s := "zrpc.Register: type " + sname + " is not exported"
		glog.Error(s)
		return errors.New(s)
	}
	if _, present := server.serviceMap[sname]; present {
		return errors.New("rpc: service already defined: " + sname)
	}
	s.name = strings.ToLower(sname)

	// Install the methods
	s.method = suitableMethods(s.typ, true)

	if len(s.method) == 0 {
		str := ""

		// To help the user, see if a pointer receiver would work.
		method := suitableMethods(reflect.PtrTo(s.typ), false)
		if len(method) != 0 {
			str = "zrpc.Register: type " + sname + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			str = "zrpc.Register: type " + sname + " has no exported methods of suitable type"
		}
		glog.Error(str)
		return errors.New(str)
	}
	server.serviceMap[s.name] = s
	glog.Infof("zrpc: registered service '%s'", s.name)
	glog.V(2).Info("zrpc: handling messages:")
	for m := range s.method {
		glog.V(2).Infof("--> %s", m)
	}
	return nil
}

func (server *Server) monitor(ctx *zmq.Context, addr string) {
	sock, err := ctx.NewSocket(zmq.PAIR)
	if err != nil {
		glog.Fatal(err)
	}

	if err := sock.Connect(addr); err != nil {
		glog.Fatal(err)
	}
	defer sock.Close()

	var lock sync.Mutex
	var numClients int

	for {
		evtType, _, _, err := sock.RecvEvent(0)
		if err != nil {
			glog.Error(err)
			break
		}

		switch zmq.Event(evtType) {
		case zmq.EVENT_ACCEPTED:
			lock.Lock()
			numClients++
			glog.Infof("Client #%d connected", numClients)
			lock.Unlock()
		case zmq.EVENT_DISCONNECTED:
			lock.Lock()
			glog.Infof("Client #%d disconnected", numClients)
			numClients--
			lock.Unlock()
		}
	}
}

// suitableMethods returns suitable Rpc methods of typ, it will report
// error using log if reportErr is true.
func suitableMethods(typ reflect.Type, reportErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported.
		if method.PkgPath != "" {
			continue
		}
		// Method can have 2 or 3 ins: receiver, *args, (optional) *reply.
		if mtype.NumIn() < 2 {
			if reportErr {
				glog.Error("zrpc.Register: method ", mname, " has wrong number of ins: ", mtype.NumIn())
			}
			continue
		}
		// First arg need not be a pointer.
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				glog.Error("zrpc.Register: ", mname, " argument type not exported: ", argType)
			}
			continue
		}
		// Second arg is optional
		var replyType reflect.Type
		if mtype.NumIn() > 2 {
			// Second arg must be a pointer.
			replyType = mtype.In(2)
			if replyType.Kind() != reflect.Ptr {
				if reportErr {
					glog.Error("zrpc.Register: method ", mname, " reply type not a pointer: ", replyType)
				}
				continue
			}
			// Reply type must be exported.
			if !isExportedOrBuiltinType(replyType) {
				if reportErr {
					glog.Error("zrpc.Register: method ", mname, " reply type not exported: ", replyType)
				}
				continue
			}
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if reportErr {
				glog.Error("zrpc.Register: method ", mname, " has wrong number of outs: ", mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				glog.Error("zrpc.Register: method ", mname, " returns ", returnType.String(), " not error")
			}
			continue
		}
		// Use the request arg as key
		var argv reflect.Value
		if argType.Kind() == reflect.Ptr {
			argv = reflect.New(argType.Elem())
		} else {
			argv = reflect.New(argType)
		}
		methods[util.GetMessageName(argv.Interface().(proto.Message))] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}

// Is this an exported - upper case - name?
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}
