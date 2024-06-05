package main

import (
	"encoding/json"
	"log"
	"os"
)

type Config struct {
	Port           int    `json:"port"`
	AppendOnly     bool   `json:"appendonly"`
	AppendFileName string `json:"appendfilename"`
}

func LoadConfig(path string) (config *Config, err error) {
	jsonStr, err := os.ReadFile(path)
	if err != nil {
		return
	}
	log.Printf("read config file: %s", jsonStr)

	config = &Config{}
	if err = json.Unmarshal(jsonStr, config); err != nil {
		return nil, err
	}
	return
}
