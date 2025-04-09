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

	var authReq k3QueryRequest
	var authResp k3QueryResponse
	authResp.RespType = "auth"
	if err := json.Unmarshal([]byte(authLine), &authReq); err != nil {
		authResp.Status = false
		authResp.Error = invalidAuthFormat
		resp, _ := json.Marshal(authResp)
		conn.Write(append(resp, '\n'))
		return
	}

	respFlag, err := checkCredentialsFiles(authReq.Database, authReq.User, authReq.Password)
	if !respFlag {
		authResp.Status = false
		authResp.Error = err.Error()
		resp, _ := json.Marshal(authResp)
		conn.Write(append(resp, '\n'))
		return
	}

	db := authReq.Database
	authResp.Status = true
	resp, _ := json.Marshal(authResp)
	conn.Write(append(resp, '\n'))
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
			result := querySQL(req.Query, db)
			output, _ := json.Marshal(result)
			conn.Write(append(output, '\n'))
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
