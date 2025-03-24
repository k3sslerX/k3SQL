package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	fmt.Print("Enter SQL query: ")
	query, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	fmt.Println(query)
	//flag := k3SQLServer.checkQuery(query)
	//fmt.Println(flag)
	//if flag {
	//	parsedQuery, err := k3SQLServer.ParseInsertQuery(query)
	//	if err == nil {
	//		fmt.Println(k3SQLServer.InsertTable(parsedQuery))
	//		//fmt.Printf("table: %s\nFields:\n", parsedQuery.table)
	//		//for k, v := range parsedQuery.fields {
	//		//	fmt.Printf("%s %d\n", k, v)
	//		//}
	//	} else {
	//		fmt.Println(err)
	//	}
	//}
}
