package k3SQLServer

import (
	"bufio"
	"fmt"
	"net"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		queryStr, err := reader.ReadString('\n')
		fmt.Println("Accepted:", queryStr)
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Println("Reading error:", err)
			}
			return
		}
		result, err := query(queryStr)
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
