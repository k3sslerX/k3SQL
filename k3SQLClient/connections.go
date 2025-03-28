package k3SQLClient

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"time"
)

func (conn K3Connection) Query(query string) (string, error) {
	reader := bufio.NewReader(conn.Conn)
	err := conn.Conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err != nil {
		return "", err
	}
	query += "\n"
	_, err = conn.Conn.Write([]byte(query))
	if err != nil {
		return "", errors.New(SendingFail)
	}
	err = conn.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		return "", err
	}
	var response []byte
	buf := make([]byte, 1024)
	for {
		n, err := reader.Read(buf)
		if err != nil {
			if err != io.EOF {
				return "", errors.New(ReadingFail)
			}
			break
		} else {
			response = append(response, buf[:n]...)
			break
		}
	}
	return string(response), nil
}

func Connect(server K3Server) (*K3Connection, error) {
	serverAddr := server.Host + ":" + strconv.Itoa(server.Port)
	connTimeout := 5 * time.Second

	conn, err := net.DialTimeout("tcp", serverAddr, connTimeout)
	if err != nil {
		return nil, err //errors.New(ConnectionError)
	}

	con := K3Connection{
		Conn:     conn,
		Database: server.Database,
	}

	return &con, nil
}
