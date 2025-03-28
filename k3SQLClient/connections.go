package k3SQLClient

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

func Connect(host, port string) {
	serverAddr := host + ":" + port
	connTimeout := 5 * time.Second

	conn, err := net.DialTimeout("tcp", serverAddr, connTimeout)
	if err != nil {
		fmt.Println("Ошибка подключения:", err)
		return
	}
	defer conn.Close()

	fmt.Printf("Подключено к %s. Введите SQL-запросы (exit для выхода):\n", serverAddr)

	scanner := bufio.NewScanner(os.Stdin)
	reader := bufio.NewReader(conn)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		query := scanner.Text()
		if query == "exit" {
			break
		}
		err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			return
		}
		query += "\n"
		_, err = conn.Write([]byte(query))
		if err != nil {
			fmt.Println("Ошибка отправки:", err)
			break
		}
		err = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			return
		}
		var response []byte
		buf := make([]byte, 1024)
		for {
			n, err := reader.Read(buf)
			if err != nil {
				if err != io.EOF {
					fmt.Println("Ошибка чтения:", err)
				}
				break
			} else {
				response = append(response, buf[:n]...)
				break
			}
		}
		fmt.Println("Ответ сервера:", string(response))
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Ошибка ввода:", err)
	}
}
