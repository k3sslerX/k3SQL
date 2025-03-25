package main

import (
	"bufio"
	"fmt"
	"k3SQLServer/k3SQLServer"
	"os"
)

func main() {
	err := k3SQLServer.StartService()
	if err == nil {
		var query string
		for {
			fmt.Print("Enter SQL query (exit - to exit): ")
			query, _ = bufio.NewReader(os.Stdin).ReadString('\n')
			if query == "exit\n" {
				break
			}
			fmt.Println(query)
			err = k3SQLServer.Query(query)
			fmt.Println(err)
		}
	} else {
		fmt.Println(err)
	}
}
