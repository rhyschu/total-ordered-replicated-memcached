// wrapper_test.go
package memcached

import (
	"testing"
)

func TestMemcachedWrapper(t *testing.T) {
	address := "localhost:21211"
	
	client, err := NewClient(address)
	if err != nil {
		t.Fatalf("Failed to connect to memcached: %v", err)
	}

	key := "test_key"
	val := "test_value"

	success := client.Set(key, val)
	if !success {
		t.Errorf("Set failed for key: %s", key)
	}

	retrievedVal, found := client.Get(key)
	if !found {
		t.Errorf("Get failed: key %s not found", key)
	}
	if retrievedVal != val {
		t.Errorf("Value mismatch: expected %s, got %s", val, retrievedVal)
	}

	_, found = client.Get("non_existent_key")
	if found {
		t.Error("Expected found=false for non_existent_key")
	}
}
