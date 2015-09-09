package main

import (
	"flag"
	"log"
	"runtime"
	"time"

	"code.google.com/p/go.net/context"
	"github.com/gogo/protobuf/proto"
	"github.com/gust1n/zrpc"
	pb "github.com/gust1n/zrpc/examples/reverseservice/reverseservicepb"
)

func main() {
	flag.Parse()
	client, err := zrpc.Dial("tcp://127.0.0.1:1337")
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	runs := 10000

	for i := 0; i < runs; i++ {
		// Create the request and response
		req := &pb.ReverseRequest{
			NormalString: proto.String("teststring"),
		}
		resp := &pb.ReverseResponse{}

		// Create the context and pass request timeout and service name
		ctx, _ := context.WithTimeout(context.Background(), time.Second*1)
		ctx = zrpc.NewServiceNameContext(ctx, "reverseservice")
		if err := client.Call(ctx, req, resp); err != nil {
			log.Println("error:", err)
		} else {
			log.Println("received:", resp)
		}

		log.Printf("%d goroutines", runtime.NumGoroutine())
		// time.Sleep(time.Millisecond * 500)
	}

	totalTime := time.Since(start)

	log.Printf("Performed %d reqs in %s (avg %s)", runs, totalTime, totalTime/time.Duration(runs))
}

func init() {
	flag.Set("logtostderr", "true")
}
