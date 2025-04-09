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
	Conn          net.Conn
	Database      string
	Authenticated bool
}

type k3Request struct {
	Action   string `json:"action"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	Query    string `json:"query"`
}

type k3Response struct {
	RespType    string              `json:"resp_type"`
	Status      bool                `json:"status"`
	Message     string              `json:"message"`
	TableFields []string            `json:"table_fields"`
	Fields      []map[string]string `json:"fields"`
	Error       string              `json:"error"`
}
