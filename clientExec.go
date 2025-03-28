package main

import (
	"bufio"
	"fmt"
	"k3SQLServer/k3SQLClient"
	"os"
	"strconv"
)

func main() {
	host := "localhost"
	port := 3003
	var err error
	if len(os.Args) == 3 {
		host = os.Args[1]
		port, err = strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	connection := k3SQLClient.K3Server{
		Host:     host,
		Port:     port,
		Database: "k3db",
		User:     "k3user",
		Password: "333",
	}
	conn, err := k3SQLClient.Connect(connection)
	if err != nil {
		fmt.Println(err)
		return
	} else {
		fmt.Println("Connected")
	}
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter SQL query (exit - for exit)")
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		query := scanner.Text()
		if query == "exit" {
			break
		}
		resp, err := conn.Query(query)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Print(resp)
		}
	}
}
