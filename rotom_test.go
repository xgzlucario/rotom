package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	assert := assert.New(t)

	t.Run("success", func(t *testing.T) {
		config, err := LoadConfig("config.json")
		assert.Nil(err)
		assert.Equal(config.Port, 6379)
	})

	t.Run("error", func(t *testing.T) {
		_, err := LoadConfig("xxxxx")
		assert.NotNil(err)

		_, err = LoadConfig("main.go")
		assert.NotNil(err)
	})
}
