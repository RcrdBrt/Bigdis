package utils

import "errors"

var (
	ErrWrongSyntax  = errors.New("wrong command syntax")
	ErrNotFound     = errors.New("(nil)")
	WrongNumberArgs = "wrong number of arguments for '%s' command"
)
