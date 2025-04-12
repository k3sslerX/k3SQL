package core

import "sync"

// FILES CONST
const K3FilesPath = "/opt/k3SQL/"
const K3ConfigurationPath = K3FilesPath + "config/"
const K3DataPath = K3FilesPath + "data/"
const Extension = ".k3"
const K3ServiceTablesPrefix = "k3_"
const K3UsersTable = K3ServiceTablesPrefix + "users"
const K3TablesTable = K3ServiceTablesPrefix + "tables"
const K3PermissionsTable = K3ServiceTablesPrefix + "permissions"

// PERMISSIONS CONST
const K3All = 0
const K3Write = 1
const K3Read = 2

// ERROR MESSAGES
const TableNotExists = "table does not exists"
const TableAlreadyExists = "table already exists"
const DatabaseNotExists = "database does not exists"
const DatabaseAlreadyExists = "database already exists"
const InvalidSQLSyntax = "SQL syntax error"
const InvalidSQLLogic = "SQL logic error"
const AccessDenied = "access denied"
const FileFormatError = "file format error"
const UserNotFound = "user not found"
const InvalidAuthFormat = "invalid auth format"
const WrongPassword = "wrong password"
const UnknownAction = "unknown action"

// DEFAULT DATABASE NAME
const DatabaseDefaultName = "k3db"

// VALUES TYPES
const K3INT = 1
const K3FLOAT = 2
const K3TEXT = 3

// VALUES ACTION
const K3DELETE = 1
const K3CREATE = 0

type K3SelectQuery struct {
	Table      *K3Table
	Values     []string
	Conditions []K3Condition
}

type K3DeleteQuery struct {
	Table      *K3Table
	Conditions []K3Condition
}

type K3UpdateQuery struct {
	Table      *K3Table
	SetValues  map[string]string
	Conditions []K3Condition
}

type K3Condition struct {
	Column   string
	Operator string
	Value    string
}

type K3join struct {
	src       string
	dst       string
	condition string
	typeJoin  int
}

type K3CreateQuery struct {
	Table       *K3Table
	Fields      map[string]int
	Constraints map[string]string
}

type K3InsertQuery struct {
	Table  *K3Table
	Values []map[string]string
}

type K3UserQuery struct {
	Database string
	Action   int
	Username string
	Password string
}

type K3Table struct {
	Database string
	Name     string
	Fields   []string
	Mu       *sync.RWMutex
}

var K3Tables map[string]*K3Table
