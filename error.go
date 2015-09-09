package zrpc

import (
	"bytes"
	"fmt"
	"net/http"
	"runtime"
	"strings"
)

type zrpcError interface {
	// This returns the error message.
	GetMessage() string

	// This returns the status code of the error.
	GetStatusCode() int

	// This returns the stack trace without the error message.
	GetStack() string

	// This returns the stack trace's context.
	GetContext() string

	// This returns the wrapped error.  This returns nil if this does not wrap
	// another error.
	GetInner() error

	// Implements the built-in error interface.
	Error() string
}

type zrpcBaseError struct {
	Msg        string
	StatusCode uint32
	Stack      string
	Context    string
	inner      error
}

// This returns a string with all available error information, including inner
// errors that are wrapped by this errors.
func (e *zrpcBaseError) Error() string {
	return DefaultError(e)
}

// This returns the error message without the stack trace.
func (e *zrpcBaseError) GetMessage() string {
	return e.Msg
}

// This returns the status code of the error.
func (e *zrpcBaseError) GetStatusCode() int {
	return int(e.StatusCode)
}

// This returns the stack trace without the error message.
func (e *zrpcBaseError) GetStack() string {
	return e.Stack
}

// This returns the stack trace's context.
func (e *zrpcBaseError) GetContext() string {
	return e.Context
}

// This returns the wrapped error, if there is one.
func (e *zrpcBaseError) GetInner() error {
	return e.inner
}

// A default implementation of the Error method of the error interface.
func DefaultError(e zrpcError) string {
	// Find the "original" stack trace, which is probably the most helpful for
	// debugging.
	errLines := make([]string, 1)
	var origStack string
	errLines[0] = "ERROR:"
	if e.GetStatusCode() != 0 {
		errLines[0] = fmt.Sprintf("ERROR (%d)", e.GetStatusCode())
	}
	fillErrorInfo(e, &errLines, &origStack)
	errLines = append(errLines, "")
	if len(origStack) > 0 {
		errLines = append(errLines, "ORIGINAL STACK TRACE:")
		errLines = append(errLines, origStack)
		return strings.Join(errLines, "\n") // Only use newlines if there is stacktrace
	}
	return strings.Join(errLines, " ")
}

// Fills errLines with all error messages, and origStack with the inner-most
// stack.
func fillErrorInfo(err error, errLines *[]string, origStack *string) {
	if err == nil {
		return
	}

	derr, ok := err.(zrpcError)
	if ok {
		*errLines = append(*errLines, derr.GetMessage())
		*origStack = derr.GetStack()
		fillErrorInfo(derr.GetInner(), errLines, origStack)
	} else {
		*errLines = append(*errLines, err.Error())
	}
}

// Returns a copy of the error with the stack trace field populated and any
// other shared initialization; skips 'skip' levels of the stack trace.
//
// NOTE: This panics on any error.
func stackTrace(skip int) (current, context string) {
	// grow buf until it's large enough to store entire stack trace
	buf := make([]byte, 128)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, len(buf)*2)
	}

	// Returns the index of the first occurrence of '\n' in the buffer 'b'
	// starting with index 'start'.
	//
	// In case no occurrence of '\n' is found, it returns len(b). This
	// simplifies the logic on the calling sites.
	indexNewline := func(b []byte, start int) int {
		if start >= len(b) {
			return len(b)
		}
		searchBuf := b[start:]
		index := bytes.IndexByte(searchBuf, '\n')
		if index == -1 {
			return len(b)
		} else {
			return (start + index)
		}
	}

	// Strip initial levels of stack trace, but keep header line that
	// identifies the current goroutine.
	var strippedBuf bytes.Buffer
	index := indexNewline(buf, 0)
	if index != -1 {
		strippedBuf.Write(buf[:index])
	}

	// Skip lines.
	for i := 0; i < skip; i++ {
		index = indexNewline(buf, index+1)
		index = indexNewline(buf, index+1)
	}

	isDone := false
	startIndex := index
	lastIndex := index
	for !isDone {
		index = indexNewline(buf, index+1)
		if (index - lastIndex) <= 1 {
			isDone = true
		} else {
			lastIndex = index
		}
	}
	strippedBuf.Write(buf[startIndex:index])
	return strippedBuf.String(), string(buf[index:])
}

// This returns the current stack trace string.  NOTE: the stack creation code
// is excluded from the stack trace.
func StackTrace() (current, context string) {
	return stackTrace(3)
}

// WrapError wraps another error in a new zrpcBaseError.
func WrapError(err error, msg string, statusCode int) zrpcError {
	stack, context := StackTrace()
	return &zrpcBaseError{
		Msg:        msg,
		StatusCode: uint32(statusCode),
		Stack:      stack,
		Context:    context,
		inner:      err,
	}
}

// This returns a new zrpcBaseError initialized with the given message and
// the current stack trace.
func NewError(msg string, statusCode int) zrpcError {
	stack, context := StackTrace()
	return &zrpcBaseError{
		Msg:        msg,
		StatusCode: uint32(statusCode),
		Stack:      stack,
		Context:    context,
	}
}

// NewClientError creates and returns a client error (without stack trace)
func NewClientError(msg string, statusCode int) zrpcError {
	return &zrpcBaseError{
		Msg:        msg,
		StatusCode: uint32(statusCode),
	}
}

// CheckError checks if the given error is of the passed status code
func CheckError(err error, statusCode int) bool {
	zrpcErr, ok := err.(zrpcError)
	if !ok {
		return false
	}
	return zrpcErr.GetStatusCode() == statusCode
}

func NewNotFoundError(msg string) zrpcError {
	return NewError(msg, http.StatusNotFound)
}

func NewInternalError(msg string) zrpcError {
	return NewError(msg, http.StatusInternalServerError)
}

func NewConflictError(msg string) zrpcError {
	return NewError(msg, http.StatusConflict)
}

func NewBadRequestError(msg string) zrpcError {
	return NewError(msg, http.StatusBadRequest)
}

func NewExpiredError(msg string) zrpcError {
	return NewError(msg, 410)
}

func NewForbiddenError(msg string) zrpcError {
	return NewError(msg, http.StatusForbidden)
}

func NewNotImplementedError(msg string) zrpcError {
	return NewError(msg, 501)
}
