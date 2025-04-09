package main

import (
	"k3SQLServer/server"
	"os"
)

func main() {
	host := "localhost"
	port := "3003"
	if len(os.Args) == 3 {
		host = os.Args[1]
		port = os.Args[2]
	}
	server.ConnectServer(host, port)
}
