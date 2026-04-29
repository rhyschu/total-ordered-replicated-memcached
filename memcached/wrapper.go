// wrapper.go
package memcached

import (
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
)

type Client struct {
	mc *memcache.Client
}

func NewClient(address string) (*Client, error) {
	mc := memcache.New(address)
	err := mc.Ping()
	if err != nil {
		fmt.Printf("memcached error: %v\n", err)
		return nil, fmt.Errorf("memcached error: %w", err)
	}
	return &Client{mc: mc}, nil
}

func (c *Client) Get(key string) (string, bool) {
	item, err := c.mc.Get(key)
	if err != nil {
		fmt.Printf("memcached Get error: %v\n", err)
		return "", false
	}
	return string(item.Value), true
}

func (c *Client) Set(key string, value string) bool {
	err := c.mc.Set(&memcache.Item{Key: key, Value: []byte(value)})
	if err != nil {
		fmt.Printf("memcached Set error: %v\n", err)
	}
	return err == nil
}
