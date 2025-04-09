package k3SQLClient

import (
	"bufio"
	"encoding/json"
	"errors"
	"golang.org/x/crypto/bcrypt"
	"io"
	"net"
	"strconv"
	"time"
)

func (conn *K3Connection) authenticate(server K3Server) error {
	password, _ := bcrypt.GenerateFromPassword([]byte(server.Password), bcrypt.DefaultCost)
	authReq := k3Request{
		Action:   "auth",
		User:     server.User,
		Password: string(password),
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

	resp := k3Response{}
	err = json.Unmarshal([]byte(response), &resp)

	if err != nil {
		return err
	} else {
		if resp.Status && resp.RespType == "auth" {
			return nil
		}
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
	req := k3Request{
		Action: "query",
		Query:  query,
	}
	reqJson, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	_, err = conn.Conn.Write(append(reqJson, '\n'))
	if err != nil {
		return "", errors.New(SendingFail)
	}
	err = conn.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		return "", err
	}
	response, err := reader.ReadString('\n')
	if err != nil {
		if err != io.EOF {
			return "", err
		}
	}
	resp := k3Response{}
	var outStr string
	err = json.Unmarshal([]byte(response), &resp)
	if err != nil {
		return "", err
	} else {
		if resp.Status && resp.RespType == "query" {
			outStr = parseOutput(resp.Fields, resp.Message, resp.TableFields)
		} else {
			outStr = resp.Error + "\n"
		}
	}
	return outStr, nil
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
