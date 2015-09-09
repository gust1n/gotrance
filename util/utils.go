package util

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
)

// GetMessageName returns a normalized name from the passed type/message
func GetMessageName(msg proto.Message) string {
	t := reflect.TypeOf(msg).Elem()
	if strings.Contains(t.Name(), ".") {
		parts := strings.Split(t.Name(), ".")
		return strings.ToLower(parts[len(parts)-1])
	}
	return strings.ToLower(t.Name())
}

// Forever loops forever running f every d.  Catches any panics, and keeps going.
func Forever(f func(), period time.Duration) {
	for {
		func() {
			defer HandleCrash()
			f()
		}()
		time.Sleep(period)
	}
}

// For testing, bypass HandleCrash.
var ReallyCrash bool

// HandleCrash simply catches a crash and logs an error. Meant to be called via defer.
func HandleCrash() {
	if ReallyCrash {
		return
	}

	r := recover()
	if r != nil {
		callers := ""
		for i := 0; true; i++ {
			_, file, line, ok := runtime.Caller(i)
			if !ok {
				break
			}
			callers = callers + fmt.Sprintf("%v:%v\n", file, line)
		}
		glog.Infof("zrpc: recovered from panic: %#v (%v)\n%v", r, r, callers)
	}
}
