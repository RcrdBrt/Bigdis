package utils

import "errors"

var (
	ErrWrongSyntax  = errors.New("wrong command syntax")
	ErrNotFound     = errors.New("(nil)")
	WrongNumberArgs = "wrong number of arguments for '%s' command"
	ErrNotInteger   = errors.New("ERR value is not an integer or out of range")
	ErrNotFloat     = errors.New("ERR value is not a valid float")
	ErrWrongType    = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)
