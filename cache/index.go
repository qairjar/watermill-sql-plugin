package cache

import (
	"github.com/jmoiron/sqlx"
	"reflect"
)

type Cache struct {
	Items []map[string]interface{}
	Count int
}

func InitCache(query string, db *sqlx.DB) (*Cache, error) {
	rows, err := db.NamedQuery(query, nil)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var cache []map[string]interface{}
	for rows.Next() {
		val := make(map[string]interface{})
		rows.MapScan(val)
		cache = append(cache, val)
	}

	return &Cache{
		Items: cache,
		Count: len(cache),
	}, nil
}

func (c *Cache) EqualMsg(msg map[string]interface{}) bool {
	for _, m := range c.Items {
		eq := reflect.DeepEqual(msg, m)
		if eq {
			return eq
		}
	}
	return false
}
