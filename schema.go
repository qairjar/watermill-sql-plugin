package sqlplugin

import (
	"database/sql"
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Adapter interface {
	MappingData(topic string, msg *message.Message) (map[string]interface{}, error)
	// UnmarshalMessage transforms the Row obtained SelectQuery a Watermill message.
	UnmarshalMessage(rows *sql.Rows) (msg *message.Message, err error)
}

type Schema struct{}

type MsgBody struct{
	After map[string]interface{}`json:"after"`
}

func (s Schema) MappingData(topic string, msg *message.Message) (map[string]interface{}, error) {
	var msgBody MsgBody
	err := json.Unmarshal(msg.Payload, &msgBody)
	if err != nil {
		return nil, err
	}
	return msgBody.After, nil
}

// UnmarshalMessage unmarshalling select query
func (s Schema) UnmarshalMessage(*sql.Rows) (msg *message.Message, err error){
	return msg, nil
}