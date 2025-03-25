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
		for err == nil {
			fmt.Print("Enter SQL query: ")
			query, _ := bufio.NewReader(os.Stdin).ReadString('\n')
			fmt.Println(query)
			err = k3SQLServer.Query(query)
		}
	}
	fmt.Println(err)
}
