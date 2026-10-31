package internet

import "syscall"

// ControlFunc operates on a socket before it is put into use. It has the same
// signature as sing's control.Func, which this fork does not import so that
// client builds stay free of GPL code.
type ControlFunc = func(network, address string, conn syscall.RawConn) error
