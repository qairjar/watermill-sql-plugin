package sqlplugin

import (
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Adapter interface {
	MappingData(topic string, msg *message.Message) (map[string]interface{}, error)
	// UnmarshalMessage transforms the Row obtained SelectQuery a Watermill message.
	UnmarshalMessage(msg *message.Message) (map[string]interface{}, error)
}

type Schema struct{}

type MsgBody struct {
	After map[string]interface{} `json:"after"`
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
func (s Schema) UnmarshalMessage(msg *message.Message) (map[string]interface{}, error) {
	msgBody := make(map[string]interface{})
	err := json.Unmarshal(msg.Payload, &msgBody)
	return msgBody, err
}
