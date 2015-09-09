package watch

import (
	"path"
	"strings"
	"time"

	"github.com/gust1n/zrpc/util"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

type etcdWatcher struct {
	client         *etcd.Client
	baseKey        string
	serviceChannel chan ServiceUpdate
	listeners      map[string][]chan ServiceUpdate
}

func NewEtcdWatcher(baseKey string, urls ...string) Watcher {
	client := etcd.NewClient(urls)

	ew := etcdWatcher{
		client:         client,
		baseKey:        baseKey,
		serviceChannel: make(chan ServiceUpdate),
		listeners:      make(map[string][]chan ServiceUpdate),
	}

	go util.Forever(func() { ew.watchEtcdForChanges(baseKey) }, time.Second)
	go util.Forever(func() { ew.handleUpdates() }, time.Second)

	return ew
}

func (ew etcdWatcher) Watch(key string, serviceChannel chan ServiceUpdate) {
	// Get current values when a new watcher registers before registering it for
	// service updates
	response, err := ew.client.Get(path.Join(ew.baseKey, key), true, true)
	if err != nil {
		glog.Error(err)
		return
	}
	for _, node := range response.Node.Nodes {
		// The only op is add since this is not a watch
		serviceUpdate := ServiceUpdate{Op: ADD, Key: key, Value: node.Value}
		// Non-blocking send
		select {
		case serviceChannel <- serviceUpdate:
		default:
			glog.Warning("notification about existing service not sent")
		}
	}

	ew.listeners[key] = append(ew.listeners[key], serviceChannel)
}

func (ew etcdWatcher) watchEtcdForChanges(baseKey string) {
	glog.Info("setting up a watch with base key: ", baseKey)
	watchChannel := make(chan *etcd.Response)
	go ew.client.Watch(baseKey, 0, true, watchChannel, nil)
	for {
		watchResponse, ok := <-watchChannel
		if !ok {
			glog.Warning("etcd watcher stopped watching")
			break
		}
		key := path.Clean(strings.TrimPrefix(watchResponse.Node.Key, baseKey))
		// Send updates about services down the serviceChannelon relevant actions
		switch watchResponse.Action {
		case "set":
			serviceUpdate := ServiceUpdate{Op: ADD, Key: key, Value: watchResponse.Node.Value}
			ew.serviceChannel <- serviceUpdate
		case "expire":
			serviceUpdate := ServiceUpdate{Op: REMOVE, Key: key, Value: watchResponse.PrevNode.Value}
			ew.serviceChannel <- serviceUpdate
		case "delete":
			serviceUpdate := ServiceUpdate{Op: REMOVE, Key: key, Value: watchResponse.PrevNode.Value}
			ew.serviceChannel <- serviceUpdate
		}
	}
}

func (ew etcdWatcher) handleUpdates() {
	// A service event occurred
	for update := range ew.serviceChannel {
		// Notify all registered listeners
		for _, listeners := range ew.listeners {
			for _, listener := range listeners {
				// Non-blocking send
				select {
				case listener <- update:
				default:
					glog.Warning("message not sent")
				}
			}
		}
	}
}
