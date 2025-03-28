package k3SQLClient

import "net"

const ConnectionError = "connection error"
const SendingFail = "sending failure"
const ReadingFail = "reading failure"

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
