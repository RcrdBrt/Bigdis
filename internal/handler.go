package internal

import (
	"fmt"
	"strconv"

	"bigdis/storage"
	"bigdis/utils"
)

type HandlerFn func(r *Request) error

func NewV1Handler() map[string]HandlerFn {
	m := make(map[string]HandlerFn)

	m["ping"] = func(r *Request) error {
		reply := &StatusReply{
			Code: "PONG",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["select"] = func(r *Request) error {
		_, err := strconv.Atoi(string(r.Args[0]))
		if err != nil {
			return utils.ErrWrongSyntax
		}

		// GetDBNum() will create a new DB if it doesn't exist
		_ = r.GetDBNum()

		reply := &StatusReply{
			Code: "OK",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["command"] = func(r *Request) error {
		reply := &StatusReply{
			Code: "Welcome to bigdis",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["get"] = func(r *Request) error {
		if len(r.Args) != 1 {
			return wrongNumberArgs(r, "get")
		}

		value, err := storage.Get(r.GetDBNum(), r.Args, nil)
		if err != nil && err != utils.ErrNotFound {
			return err
		}

		reply := &BulkReply{
			value: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["set"] = func(r *Request) error {
		if len(r.Args) < 2 {
			return wrongNumberArgs(r, "set")
		}

		if err := storage.Set(r.GetDBNum(), r.Args, nil); err != nil {
			return err
		}

		reply := &StatusReply{
			Code: "OK",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["flushdb"] = func(r *Request) error {
		if len(r.Args) > 1 {
			return wrongNumberArgs(r, "flushdb")
		}

		if err := storage.FlushDB(r.GetDBNum(), r.Args); err != nil {
			return err
		}

		reply := &StatusReply{
			Code: "OK",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["del"] = func(r *Request) error {
		if len(r.Args) < 1 {
			return wrongNumberArgs(r, "del")
		}

		deleted, err := storage.Del(r.GetDBNum(), r.Args, nil)
		if err != nil {
			return err
		}

		reply := IntegerReply{
			number: deleted,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["config"] = func(r *Request) error {
		reply := BulkReply{
			value: []byte(""),
		}

		// fmt.Println(string(r.Args[1]))

		// switch string(r.Args[0]) {
		// case "get":
		// 	switch string(r.Args[1]) {
		// 	case "save":
		// 		reply.value = ""
		// 	}
		// }

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["getdel"] = func(r *Request) error {
		if len(r.Args) != 1 {
			return wrongNumberArgs(r, "getdel")
		}

		value, err := storage.GetDel(r.GetDBNum(), r.Args)
		if err != nil {
			return err
		}

		reply := &BulkReply{
			value: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["exists"] = func(r *Request) error {
		if len(r.Args) < 1 {
			return wrongNumberArgs(r, "exists")
		}

		count, err := storage.Exists(r.GetDBNum(), r.Args, nil)
		if err != nil {
			return err
		}

		reply := &IntegerReply{
			number: count,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["incr"] = func(r *Request) error {
		if len(r.Args) != 1 {
			return wrongNumberArgs(r, "incr")
		}

		value, err := storage.Incr(r.GetDBNum(), r.Args)
		if err != nil {
			return err
		}

		reply := &IntegerReply{
			number: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["incrby"] = func(r *Request) error {
		if len(r.Args) != 2 {
			return wrongNumberArgs(r, "incrby")
		}

		value, err := storage.IncrBy(r.GetDBNum(), r.Args, nil)
		if err != nil {
			return err
		}

		reply := &IntegerReply{
			number: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["getset"] = func(r *Request) error {
		if len(r.Args) != 2 {
			return wrongNumberArgs(r, "getset")
		}

		value, err := storage.GetSet(r.GetDBNum(), r.Args)
		if err != nil {
			return err
		}

		reply := &BulkReply{
			value: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["flushall"] = func(r *Request) error {
		if len(r.Args) > 1 {
			return wrongNumberArgs(r, "flushall")
		}

		if err := storage.FlushAll(r.Args); err != nil {
			return err
		}

		reply := &StatusReply{
			Code: "OK",
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["strlen"] = func(r *Request) error {
		if len(r.Args) != 1 {
			return wrongNumberArgs(r, "strlen")
		}

		value, err := storage.Strlen(r.GetDBNum(), r.Args)
		if err != nil {
			return err
		}

		reply := &IntegerReply{
			number: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["append"] = func(r *Request) error {
		if len(r.Args) != 2 {
			return wrongNumberArgs(r, "append")
		}

		value, err := storage.Append(r.GetDBNum(), r.Args)
		if err != nil {
			return err
		}

		reply := &IntegerReply{
			number: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["decr"] = func(r *Request) error {
		if len(r.Args) != 1 {
			return wrongNumberArgs(r, "decr")
		}

		value, err := storage.Decr(r.GetDBNum(), r.Args)
		if err != nil {
			return err
		}

		reply := &IntegerReply{
			number: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	m["decrby"] = func(r *Request) error {
		if len(r.Args) != 2 {
			return wrongNumberArgs(r, "decrby")
		}

		value, err := storage.DecrBy(r.GetDBNum(), r.Args)
		if err != nil {
			return err
		}

		reply := &IntegerReply{
			number: value,
		}

		if _, err := reply.WriteTo(r.Conn); err != nil {
			return err
		}

		return nil
	}

	return m
}

func wrongNumberArgs(r *Request, cmd string) error {
	value := fmt.Sprintf(utils.WrongNumberArgs, cmd)

	reply := &ErrorReply{
		value: value,
	}

	if _, err := reply.WriteTo(r.Conn); err != nil {
		return err
	}

	return nil
}
