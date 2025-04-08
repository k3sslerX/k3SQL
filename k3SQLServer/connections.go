package k3SQLServer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
)

type k3QueryRequest struct {
	Action   string `json:"action"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	Query    string `json:"query"`
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	fmt.Printf("incoming connection from %v\n", conn.RemoteAddr())

	authLine, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	var authReq k3QueryRequest
	if err := json.Unmarshal([]byte(authLine), &authReq); err != nil {
		conn.Write([]byte(invalidAuthFormat + "\n"))
		return
	}

	resp, err := checkCredentialsFiles(authReq.Database, authReq.User, authReq.Password)
	if !resp {
		conn.Write([]byte(err.Error() + "\n"))
		return
	}

	db := authReq.Database

	conn.Write([]byte("OK\n"))
	for {
		reqStr, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() != "EOF" {
				conn.Write([]byte(err.Error() + "\n"))
			}
			return
		}
		var req k3QueryRequest
		err = json.Unmarshal([]byte(reqStr), &req)
		if err != nil {
			conn.Write([]byte(err.Error() + "\n"))
		}
		if req.Action == "query" {
			result, err := querySQL(req.Query, db)
			if err == nil {
				conn.Write([]byte(result))
			} else {
				conn.Write([]byte(err.Error() + "\n"))
			}
			if err != nil {
				fmt.Println("Sending error:", err)
				return
			}
		} else {
			conn.Write([]byte(unknownAction + "\n"))
		}
	}
}

func ConnectServer(host, port string) {
	err := startService()
	if err != nil {
		fmt.Println("Can't start K3SQLServer service")
		return
	}
	serverAddr := host + ":" + port
	listener, err := net.Listen("tcp", serverAddr)
	if err != nil {
		fmt.Println("Server not started. Error:", err)
		return
	}
	defer listener.Close()
	fmt.Println("K3SQLServer started on", serverAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}
		go handleConnection(conn)
	}
}
