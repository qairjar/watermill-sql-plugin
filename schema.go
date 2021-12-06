package sqlplugin

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"strings"
)

type Adapter interface {
	MappingData(topic string, msg *message.Message) (string, map[string]interface{}, error)
	// UnmarshalMessage transforms the Row obtained SelectQuery a Watermill message.
	UnmarshalMessage(rows *sql.Rows) (msg *message.Message, err error)
}

type Schema struct{}

type MsgBody struct{
	After map[string]interface{}`json:"after"`
}

func (s Schema) MappingData(topic string, msg *message.Message) (string, map[string]interface{}, error) {
	var msgBody MsgBody
	err := json.Unmarshal(msg.Payload, &msgBody)
	if err != nil {
		return "", nil, err
	}
	insertKeys := []string{}
	for key := range msgBody.After {
		insertKeys = append(insertKeys, key)
	}
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (:%s)`,
		topic,
		strings.Join(insertKeys, ","),
		strings.Join(insertKeys, ",:"),
	)
	return insertQuery, msgBody.After, nil
}

// UnmarshalMessage unmarshalling select query
func (s Schema) UnmarshalMessage(*sql.Rows) (msg *message.Message, err error) {

	return msg, nil
}