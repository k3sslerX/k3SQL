package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

func main() {
	arguments := os.Args
	port := 3003
	if len(arguments) > 1 {
		tmp, err := strconv.Atoi(arguments[1])
		if err != nil {
			fmt.Printf("usage: %s [port]\n", arguments[0])
			return
		} else {
			port = tmp
		}
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: []byte{0, 0, 0, 0}, Port: port})
	if err != nil {
		fmt.Println("error while establishing server")
		return
	}
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Println(err)
		}
		fmt.Println("Получено: ", string(buf[:n]))
	}
}
