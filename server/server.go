package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"bigdis/config"
	"bigdis/internal"
)

type server struct {
	host         string
	port         int
	monitorChans []chan string
	methods      map[string]internal.HandlerFn
	listener     *net.TCPListener
}

func StartServer() error {
	srv := &server{
		host:         config.Config.Server.Host,
		port:         config.Config.Server.Port,
		monitorChans: []chan string{},
	}

	srv.methods = internal.NewV1Handler()

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP:   net.ParseIP(srv.host),
		Port: srv.port,
	})
	if err != nil {
		return err
	}
	defer listener.Close()

	srv.listener = listener

	for {
		conn, err := srv.listener.AcceptTCP()
		if err != nil {
			return err
		}

		go srv.serveClient(conn)
	}
}

func (srv *server) serveClient(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(conn, "-%s\r\n", err)
		}
		if err := conn.Close(); err != nil {
			log.Println(err)
		}
	}()

	reader := bufio.NewReader(conn)
	dbNum := [][]byte{[]byte("0")}
	for {
		request, err := parseRequest(reader)
		if err != nil {
			panic(err)
		}
		request.Conn = conn

		if request.Name == "select" {
			dbNum = request.Args
		}
		request.DB = dbNum

		if request.Name == "quit" {
			fmt.Fprint(conn, "+OK\r\n")
			return
		}

		// check existence of command
		if _, exists := srv.methods[request.Name]; !exists {
			// build args string to respect redis protocol
			args := ""
			for _, arg := range request.Args {
				args += fmt.Sprintf("'%s' ", arg)
			}
			args = strings.TrimRight(args, " ")

			reply := &internal.StatusReply{
				Code: fmt.Sprintf("ERR unknown command '%s', with args beginning with: %s", request.Name, args),
			}

			if _, err := reply.WriteTo(conn); err != nil {
				panic(err)
			}
			continue
		}

		if err := srv.methods[request.Name](request); err != nil {
			panic(err)
		}
	}
}
