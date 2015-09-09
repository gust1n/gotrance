package main

import (
	"flag"

	"github.com/gogo/protobuf/proto"
	"github.com/gust1n/zrpc"
	pb "github.com/gust1n/zrpc/examples/reverseservice/reverseservicepb"
)

type Reverseservice int

func (e *Reverseservice) HandleReverse(req *pb.ReverseRequest, resp *pb.ReverseResponse) error {
	// Get Unicode code points.
	n := 0
	rune := make([]rune, len(req.GetNormalString()))
	for _, r := range req.GetNormalString() {
		rune[n] = r
		n++
	}
	rune = rune[0:n]
	// Reverse
	for i := 0; i < n/2; i++ {
		rune[i], rune[n-1-i] = rune[n-1-i], rune[i]
	}
	// Convert back to UTF-8.
	resp.ReversedString = proto.String(string(rune))

	// No errors occurred
	return nil
}

func main() {
	flag.Parse()
	service := new(Reverseservice)
	server := zrpc.NewServer(1)
	server.Register(service)
	server.Start("tcp://127.0.0.1:1337")
}

func init() {
	flag.Set("logtostderr", "true")
}
