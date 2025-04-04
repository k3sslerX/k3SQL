package k3SQLServer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	fmt.Printf("incoming connection from %v\n", conn.RemoteAddr())

	authLine, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	var authReq k3AuthRequest
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
		queryStr, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Println("Reading error:", err)
			}
			return
		}
		result, err := querySQL(queryStr, db)
		if err == nil {
			_, err = fmt.Fprintln(conn, result)
		} else {
			_, err = fmt.Fprintln(conn, err)
		}
		if err != nil {
			fmt.Println("Sending error:", err)
			return
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
