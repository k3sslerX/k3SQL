package k3SQLServer

import "sync"

// FILES CONST
const k3FilesPath = "/opt/k3SQL/"
const k3configurationPath = k3FilesPath + "config/"
const k3sqlDataPath = k3FilesPath + "data/"
const extension = ".k3"

// VALUES TYPES
const k3INT = 1
const k3FLOAT = 2
const k3TEXT = 3

// VALUES ACTION
const k3DELETE = 1
const k3CREATE = 0

// ERROR MESSAGES
const tableNotExists = "table does not exists"
const tableAlreadyExists = "table already exists"
const databaseNotExists = "database does not exists"
const databaseAlreadyExists = "database already exists"
const invalidSQLSyntax = "SQL syntax error"
const invalidSQLLogic = "SQL logic error"
const accessDenied = "access denied"
const fileFormatError = "file format error"
const userNotFound = "user not found"
const invalidAuthFormat = "invalid auth format"
const wrongPassword = "wrong password"
const unknownAction = "unknown action"

// DEFAULT DATABASE NAME
const databaseDefaultName = "k3db"

// META DATA

type k3Table struct {
	database string
	name     string
	fields   []string
	mu       *sync.RWMutex
}

type k3SelectQuery struct {
	table      *k3Table
	values     []string
	conditions []k3Condition
	//join      *k3join
}

type k3DeleteQuery struct {
	table      *k3Table
	conditions []k3Condition
}

type k3UpdateQuery struct {
	table      *k3Table
	setValues  map[string]string
	conditions []k3Condition
}

type k3Condition struct {
	column   string
	operator string
	value    string
}

type k3join struct {
	src       string
	dst       string
	condition string
	typeJoin  int
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

type k3UserQuery struct {
	database string
	action   int
	username string
	password string
}

var k3Tables map[string]*k3Table
