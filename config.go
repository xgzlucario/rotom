package main

import (
	"encoding/json"
	"os"
)

type Config struct {
	Port           int    `json:"port"`
	AppendOnly     bool   `json:"appendonly"`
	AppendFileName string `json:"appendfilename"`
	Save           bool   `json:"save"`
	SaveFileName   string `json:"savefilename"`
}

func LoadConfig(path string) (config *Config, err error) {
	jsonStr, err := os.ReadFile(path)
	if err != nil {
		return
	}
	config = &Config{}
	if err = json.Unmarshal(jsonStr, config); err != nil {
		return nil, err
	}
	return
}
