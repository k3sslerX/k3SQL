package main

import (
	"bufio"
	"fmt"
	"k3SQLClient"
	"os"
	"strings"
)

func main() {
	server := k3SQLClient.K3Server{
		Host:     "localhost",
		Port:     3003,
		Database: "k3db",
		User:     "k3user",
		Password: "333",
	}
	conn, err := k3SQLClient.Connect(server)
	if err == nil {
		fmt.Println("Connected")
		fmt.Println("Enter SQL query (exit - to exit): ")
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("> ")
			queryMsg, _ := reader.ReadString('\n')
			queryMsg = strings.TrimSuffix(queryMsg, "\n")
			if queryMsg == "exit" {
				break
			}
			resp, err := conn.Query(queryMsg)
			if err == nil {
				fmt.Print(resp)
			} else {
				fmt.Println(err)
			}
		}
	}
}
