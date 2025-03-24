package k3SQLServer

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

type K3join struct {
	Src       string
	Dst       string
	Condition string
	TypeJoin  int
}

type K3query struct {
	Table     string
	Values    []string
	Condition string
	Join      *K3join
}

func CheckQuery(queryStr *string) bool {
	parts := strings.Fields(*queryStr)
	selectFlag := false
	fromFlag := false
	joinFlag := false
	orderFlag := false
	for _, part := range parts {
		if strings.EqualFold(part, "select") {
			selectFlag = true
		} else if strings.EqualFold(part, "from") {
			if !selectFlag {
				return false
			}
			fromFlag = true
		} else if strings.EqualFold(part, "join") {
			if !selectFlag || !fromFlag {
				return false
			}
			joinFlag = true
		} else if strings.EqualFold(part, "on") {
			if !selectFlag || !fromFlag || !joinFlag {
				return false
			}
		} else if strings.EqualFold(part, "where") {
			if !selectFlag || !fromFlag {
				return false
			}
		} else if strings.EqualFold(part, "order") {
			if !selectFlag || !fromFlag {
				return false
			}
			orderFlag = true
		} else if strings.EqualFold(part, "by") {
			if !selectFlag || !fromFlag || !orderFlag {
				return false
			}
		}
	}
	if selectFlag && fromFlag {
		return true
	}
	return false
}

func ParseQuery(queryStr *string) (*K3query, error) {
	parts := strings.Fields(*queryStr)
	query := new(K3query)
	join := new(K3join)
	query.Values = make([]string, 0)
	selectCond := false
	fromCond := false
	whereCond := false
	joinCond := false
	joinFlag := false
	onCond := false
	onFlag := false
	for _, part := range parts {
		if strings.EqualFold(part, "select") {
			selectCond = true
			continue
		} else if strings.EqualFold(part, "from") {
			fromCond = true
			selectCond = false
			continue
		} else if strings.EqualFold(part, "join") {
			joinCond = true
			joinFlag = true
			continue
		} else if strings.EqualFold(part, "on") {
			joinCond = false
			onCond = true
			onFlag = true
			continue
		} else if strings.EqualFold(part, "where") {
			onCond = false
			whereCond = true
			continue
		}
		if selectCond {
			query.Values = append(query.Values, part)
		} else if fromCond {
			query.Table = part
			fromCond = false
		} else if whereCond {
			query.Condition += part
		} else if joinCond {
			join.Src = query.Table
			join.Dst = part
		} else if onCond {
			join.Condition += part
		}
	}
	if (joinFlag && !onFlag) || (!joinFlag && onFlag) || len(query.Values) == 0 || len(query.Table) == 0 {
		return nil, errors.New("SQL syntax error")
	}
	if !joinFlag {
		query.Join = nil
	} else {
		query.Join = join
	}
	return query, nil
}

func connectServer() {
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
	for {
		buf := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Println(err)
		}
		fmt.Println("Получено: ", string(buf[:n]))
	}
}
