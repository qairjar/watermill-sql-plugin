package sqlplugin

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/gocql/gocql"
	"strings"
	"time"
)

type Adapter interface {
	MappingData(topic string, msg *message.Message) (string, []interface{}, error)
	// UnmarshalMessage transforms the Row obtained SelectQuery a Watermill message.
	UnmarshalMessage(rows *sql.Rows) (msg *message.Message, err error)
}

type Schema struct{}
type Model struct {
	UserID    gocql.UUID `json:"user_id"`
	M         map[string]interface{}
	createdAt time.Time
}

func (s Schema) MappingData(topic string, msg *message.Message) (string, []interface{}, error) {
	data := make(map[string]interface{})
	err := json.Unmarshal(msg.Payload, &data)
	if err != nil {
		return "", nil, err
	}
	var insertKeys []string
	var args []interface{}
	i := 1
	var columnCount string
	for key, value := range data {
		insertKeys = append(insertKeys, key)
		args = append(args, value)
		if i != 1 {
			columnCount += ","
		}
		columnCount += fmt.Sprintf("$%d", i)
		i++
	}

	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		topic,
		strings.Join(insertKeys, ","),
		columnCount,
	)
	return insertQuery, args, nil
}

// UnmarshalMessage unmarshalling select query
func (s Schema) UnmarshalMessage(rows *sql.Rows) (msg *message.Message, err error) {

	return msg, nil
}
