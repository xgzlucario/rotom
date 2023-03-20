package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/structx"
)

func TestTrie(t *testing.T) {
	trie := structx.NewTrie[int]()

	// Test Put and Get
	trie.Put("hello", 1)
	v, ok := trie.Get("hello")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	// Test Contains
	assert.True(t, trie.Contains("hello"))
	assert.False(t, trie.Contains("world"))

	// Test Remove
	trie.Remove("hello")
	assert.False(t, trie.Contains("hello"))

	// Test Keys
	trie.Put("abc", 2)
	trie.Put("def", 3)
	keys := trie.Keys()
	assert.ElementsMatch(t, []string{"abc", "def"}, keys)

	// Test KeysWithPrefix
	keysWithPrefix := trie.KeysWithPrefix("a")
	assert.ElementsMatch(t, []string{"abc"}, keysWithPrefix)

	// Test MarshalJSON and UnmarshalJSON
	trie.Put("xyz", 4)
	data, err := trie.MarshalJSON()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	newTrie := structx.NewTrie[int]()
	assert.NoError(t, newTrie.UnmarshalJSON(data))
	assert.Equal(t, trie.Size(), newTrie.Size())
	assert.True(t, newTrie.Contains("xyz"))
}
