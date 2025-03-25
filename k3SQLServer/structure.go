package k3SQLServer

import "sync"

// FILES CONST
const k3FilesPath = "k3SQL/"
const k3configurationPath = k3FilesPath + "config/"
const k3sqlDataPath = k3FilesPath + "data/"
const extension = ".k3"

// VALUES TYPES
const k3INT = 1
const k3FLOAT = 2
const k3TEXT = 3

// ERROR MESSAGES
const tableNotExists = "table does not exists"
const tableExists = "table already exists"
const databaseNotExists = "database does not exists"

// DEFAULT DATABASE NAME
const databaseDefaultName = "k3db"

// META DATA
type k3Table struct {
	database string
	name     string
	//fields map[string]int
	mu *sync.RWMutex
}

type k3join struct {
	src       string
	dst       string
	condition string
	typeJoin  int
}

type k3SelectQuery struct {
	table     *k3Table
	values    []string
	condition string
	//join      *k3join
}

type k3CreateQuery struct {
	table       *k3Table
	fields      map[string]int
	constraints map[string]string
}

type k3InsertQuery struct {
	table  *k3Table
	values []map[string]string
}

var k3Tables map[string]*k3Table
