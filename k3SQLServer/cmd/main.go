package main

import (
	"k3SQLServer"
	"os"
)

func main() {
	host := "localhost"
	port := "3003"
	if len(os.Args) == 3 {
		host = os.Args[1]
		port = os.Args[2]
	}
	k3SQLServer.ConnectServer(host, port)
}
