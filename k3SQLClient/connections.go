package k3SQLClient

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"time"
)

func (conn *K3Connection) authenticate(server K3Server) error {
	authReq := K3AuthRequest{
		Action:   "auth",
		User:     server.User,
		Password: server.Password,
		Database: server.Database,
	}

	authData, err := json.Marshal(authReq)
	if err != nil {
		return err
	}

	err = conn.Conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err != nil {
		return err
	}

	_, err = conn.Conn.Write(append(authData, '\n'))
	if err != nil {
		return errors.New(SendingFail)
	}

	err = conn.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		return err
	}

	reader := bufio.NewReader(conn.Conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return errors.New(ReadingFail)
	}

	if response != "OK\n" {
		return errors.New(AuthFail)
	}

	conn.Authenticated = true
	return nil
}

func (conn *K3Connection) Query(query string) (string, error) {
	if conn.Conn == nil {
		return "", errors.New(ConnectionIsNotSet)
	}
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
		return nil, errors.New(ConnectionError)
	}

	k3conn := &K3Connection{
		Conn:     conn,
		Database: server.Database,
	}

	if err := k3conn.authenticate(server); err != nil {
		conn.Close()
		return nil, err
	}

	return k3conn, nil
}
