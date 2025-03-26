package k3SQLServer

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

func ConnectServer() {
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
	err = StartService()
	if err != nil {
		fmt.Println("error while starting k3SQL service")
		return
	}
	fmt.Printf("k3SQL service started on port %d\n", port)
	for {
		buf := make([]byte, 2048)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Println(err)
		}
		query := string(buf[:n])
		fmt.Println("Получено: ", query)
		go func() {
			resp, err := Query(query)
			if err == nil {
				fmt.Println(resp)
			} else {
				fmt.Println(err)
			}
		}()
	}
}
