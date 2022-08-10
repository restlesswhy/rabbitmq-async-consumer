package config

import (
	"log"

	"github.com/spf13/viper"
)

type Config struct {
	PullQueue string `mapstructure:"PULL_QUEUE"`
	PushQueue string `mapstructure:"PUSH_QUEUE"`
	Addr      string `mapstructure:"ADDR"`
}

func Load() *Config {
	env := &Config{}
	viper.SetConfigFile(".env")

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal("cannot read cofiguration")
	}

	err = viper.Unmarshal(&env)
	if err != nil {
		log.Fatal("environment cant be loaded: ", err)
	}

	return env
}
