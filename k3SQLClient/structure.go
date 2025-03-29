package k3SQLClient

import "net"

const (
	ConnectionError    = "connection error"
	SendingFail        = "sending failure"
	ReadingFail        = "reading failure"
	ConnectionIsNotSet = "connection is not set"
	AuthFail           = "authentication failure"
)

type K3Server struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

type K3Connection struct {
	Conn          net.Conn
	Database      string
	Authenticated bool
}

type K3AuthRequest struct {
	Action   string `json:"action"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}
