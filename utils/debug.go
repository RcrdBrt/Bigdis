package utils

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

var Stderr = io.Writer(os.Stderr)

// This closure is a no-op unless the DEBUG env is non empty.
var Print = func(format string, a ...interface{}) {}

func init() {
	if os.Getenv("DEBUG") != "" {
		Print = ActualDebugf
	}

}

// If Docker is in damon mode, also send the debug info on the socket
// Convenience debug function, courtesy of http://github.com/dotcloud/docker
func ActualDebugf(format string, a ...interface{}) {
	// Retrieve the stack infos
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "<unknown>"
		line = -1
	} else {
		file = file[strings.LastIndex(file, string(filepath.Separator))+1:]
	}
	fmt.Fprintf(Stderr, "[%d] [debug] %s:%d ", os.Getpid(), file, line)
	fmt.Fprintf(Stderr, format, a...)
	fmt.Fprintln(Stderr)
}
