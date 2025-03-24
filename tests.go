package main

import (
	"bufio"
	"fmt"
	"k3SQLServer/k3SQLServer"
	"os"
)

func main() {
	fmt.Print("Enter SQL query: ")
	query, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	fmt.Println(query)
	flag := k3SQLServer.CheckQuery(&query)
	fmt.Println(flag)
	if flag {
		parsedQuery, err := k3SQLServer.ParseQuery(&query)
		if err == nil {
			fmt.Printf("table: %s\nvalues: ", parsedQuery.Table)
			for _, value := range parsedQuery.Values {
				fmt.Printf("%s ", value)
			}
			fmt.Printf("\ncodnition: %s", parsedQuery.Condition)
		} else {
			fmt.Println(err)
		}
	}
}
