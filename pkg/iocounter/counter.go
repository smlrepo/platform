// Package iocounter provides an io.Writer that tracks how many bytes have been written to it.
package iocounter

// Counter counts a number of bytes during an IO operation.
type Counter interface {
	Count() int64
}
