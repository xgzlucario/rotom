package main

import (
	"github.com/spf13/viper"
)

const (
	defaultConfigFileName = "rotom.toml"
)

func initConfig(fileName string) error {
	viper.SetConfigFile(fileName)
	return viper.ReadInConfig()
}

func configGet(key string) any { return viper.Get(key) }

func configGetString(key string) string { return viper.GetString(key) }

func configGetInt(key string) int { return viper.GetInt(key) }

func configGetBool(key string) bool { return viper.GetBool(key) }

func configGetPort() int {
	return configGetInt("tcp.port")
}

func configGetAppendOnly() bool {
	return configGetBool("aof.appendonly")
}

func configGetAppendFileName() string {
	return configGetString("aof.appendfilename")
}

func configGetDbFileName() string {
	return configGetString("rdb.dbfilename")
}
