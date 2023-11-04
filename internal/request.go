package internal

import (
	"bigdis/storage"
	"net"
	"strconv"
)

type Request struct {
	DB   [][]byte
	Name string
	Args [][]byte
	Conn net.Conn
}

func (r *Request) GetDBNum() int {
	if len(r.DB) < 1 {
		return 0
	}

	dbNum, err := strconv.Atoi(string(r.DB[0]))
	if err != nil {
		return 0
	}

	_, exists := storage.AvailableDBs[dbNum]
	if !exists {
		if err := storage.NewDB(dbNum); err != nil {
			reply := &StatusReply{
				Code: err.Error(),
			}

			if _, err := reply.WriteTo(r.Conn); err != nil {
				panic(err)
			}
		}
	}

	return dbNum
}
