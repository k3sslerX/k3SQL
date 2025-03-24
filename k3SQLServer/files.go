package k3SQLServer

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const K3sqlDataPath = k3FilesPath + "data/"
const extension = ".k3"

const K3INT = 1
const K3FLOAT = 2
const K3TEXT = 3

func createTableFile(name string, fields map[string]int) error {
	file, err := os.Create(K3sqlDataPath + name + extension)
	defer file.Close()
	if err == nil {
		writer := bufio.NewWriter(file)
		str := "|"
		for k, v := range fields {
			str += fmt.Sprintf("%d %s|", v, k)
		}
		_, err = writer.WriteString(str)
		if err != nil {
			return err
		}
		err = writer.Flush()
		if err != nil {
			return err
		}
	}
	return err
}

func insertTableFile(name string, values []map[string]string) error {
	file, err := os.Open(name)
	defer file.Close()
	if err == nil {
		data := make([]byte, 128)
		_, err := file.Read(data)
		if err != nil {
			return err
		}
		var tableTypes map[string]int
		dataStr := string(data)
		parts := strings.Split(dataStr, "|")
		for _, part := range parts {
			tableType, err := strconv.Atoi(string(part[0]))
			if err != nil {
				return err
			}
			tableTypes[part[2:]] = tableType
		}
		writer := bufio.NewWriter(file)
		for _, value := range values {
			str := ""
			for k, v := range value {
				if tableTypes[k] == K3INT {
					_, err := strconv.Atoi(v)
					if err != nil {
						return err
					}
					str += v + "|"
				} else if tableTypes[k] == K3FLOAT {
					_, err := strconv.ParseFloat(v, 64)
					if err != nil {
						return err
					}
					str += v + "|"
				} else if tableTypes[k] == K3TEXT {
					str += v + "|"
				} else {
					return errors.New("unknown type")
				}
			}
			writer.WriteString(str)
		}
		writer.Flush()
	}
	return err
}
