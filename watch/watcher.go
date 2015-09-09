package watch

type Watcher interface {
	// Watch register passed chan for service updates for a key
	Watch(key string, serviceChannel chan ServiceUpdate)
}
