package k3SQLClient

import "net"

const (
	ConnectionError    = "connection error"
	SendingFail        = "sending failure"
	ReadingFail        = "reading failure"
	ConnectionIsNotSet = "connection is not set"
)

type K3Server struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

type K3Connection struct {
	Conn     net.Conn
	Database string
}
