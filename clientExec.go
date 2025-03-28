package main

import (
	"k3SQLServer/k3SQLClient"
	"os"
)

func main() {
	host := "localhost"
	port := "3003"
	if len(os.Args) == 3 {
		host = os.Args[1]
		port = os.Args[2]
	}
	k3SQLClient.Connect(host, port)
}
