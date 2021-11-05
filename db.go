package sqlplugin

import (
	"database/sql"
	"fmt"
	_ "github.com/MichaelS11/go-cql-driver"
	"github.com/ThreeDotsLabs/watermill"
	"time"
)

type SQLConfig struct {
	Type           string
	Host           string
	User           string
	Pass           string
	EnableAuth     bool
	TimeoutValid   time.Duration
	ConnectTimeout time.Duration
	Logger         watermill.LoggerAdapter
	Topic          string
	Keyspace       string
	Consistency    string
}

// CreateDB connecting with sql database
func (c *SQLConfig) CreateDB() (*sql.DB, error) {
	openString := fmt.Sprintf("%s?timeout=%s&connectTimeout=%s&enableHostVerification=true", c.Host, c.TimeoutValid, c.ConnectTimeout)
	if c.EnableAuth {
		openString += fmt.Sprintf("&username=%s&password=%s", c.User, c.Pass)
	}
	openString += fmt.Sprintf("&keyspace=%s", c.Keyspace)
	openString += fmt.Sprintf("&consistency=%s", c.Consistency)

	db, err := sql.Open(c.Type, openString)
	if err != nil {
		fmt.Printf("Open error is not nil: %v", err)
		return nil, err
	}
	if db == nil {
		fmt.Println("db is nil")
		return nil, err
	}

	return db, nil
}
