package zrpc

import (
	"log"
	"testing"
	"time"

	pb "github.com/gust1n/zrpc/examples/reverseservice/reverseservicepb"

	"code.google.com/p/go.net/context"
	"github.com/gogo/protobuf/proto"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type BaseSuite struct {
	server *Server
	client *Client
	req    proto.Message
	resp   proto.Message
	ctx    context.Context
}

type TCPSuite struct {
	BaseSuite
}

var _ = Suite(&TCPSuite{})

func (s *TCPSuite) BenchmarkTCP(c *C) {
	for i := 0; i < c.N; i++ {
		if err := s.client.Call(s.ctx, s.req, s.resp); err != nil {
			log.Fatal(err)
		}
	}
}

func (s *BaseSuite) SetUpTest(c *C, clientAddr string) {
	if s.client == nil {
		log.Println("setting up client at:", clientAddr)
		s.client, _ = Dial(clientAddr)
	}

	s.req = &pb.ReverseRequest{
		NormalString: proto.String("test"),
	}
	s.resp = &pb.ReverseResponse{}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	s.ctx = NewServiceNameContext(ctx, "reverseservice")
}

func (s *BaseSuite) SetUpSuite(c *C, numWorkers int, serverAddr string) {
	log.Println("setting up server at:", serverAddr)
	s.server = NewServer(numWorkers)
	s.server.Register(new(ReverseService))
	go s.server.Start(serverAddr)
	time.Sleep(time.Second)
}

func (s *TCPSuite) SetUpSuite(c *C) {
	s.BaseSuite.SetUpSuite(c, 1, "tcp://127.0.0.1:9909")
}
func (s *TCPSuite) SetUpTest(c *C) {
	s.BaseSuite.SetUpTest(c, "tcp://127.0.0.1:9909")
}

func (s *TCPSuite) TearDownSuite(c *C) {
	s.client.Close()
	s.server.Close()
	time.Sleep(time.Second * 1)
}

type ReverseService int

func (t *ReverseService) HandleReverse(req *pb.ReverseRequest, resp *pb.ReverseResponse) error {
	resp.ReversedString = proto.String("tset")
	return nil
}
