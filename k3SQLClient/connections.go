package k3SQLClient

import (
	"log"
	"net"
	"os"
	"strconv"
)

func Connect() {
	args := os.Args
	port := 3003
	if len(args) > 1 {
		tmpPort, err := strconv.Atoi(args[1])
		if err == nil {
			port = tmpPort
		}
	}
	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: []byte{127, 0, 0, 1}, Port: port})
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()
	message := "select * from cars"
	_, err = conn.Write([]byte(message))
	if err != nil {
		log.Println(err)
	}
}
