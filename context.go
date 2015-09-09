package zrpc

import "code.google.com/p/go.net/context"

// The key type is unexported to prevent collisions with context keys defined in
// other packages.
type key int

// userIPkey is the context key for the user IP address.  Its value of zero is
// arbitrary.  If this package defined other context keys, they would have
// different integer values.
const serviceNameKey key = 0

// NewServiceNameContext returns a new context carrying a service name
func NewServiceNameContext(ctx context.Context, serviceName string) context.Context {
	return context.WithValue(ctx, serviceNameKey, serviceName)
}

// ServiceNameFromContext extracts a servicename from passed context
func ServiceNameFromContext(ctx context.Context) (string, bool) {
	// ctx.Value returns nil if ctx has no value for the key;
	// the string type assertion returns ok=false for nil.
	serviceName, ok := ctx.Value(serviceNameKey).(string)
	return serviceName, ok
}
