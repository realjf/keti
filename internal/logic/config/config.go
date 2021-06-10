package config

import (
	"flag"
	"os"
	"strconv"

	"github.com/BurntSushi/toml"
)

var (
	configPath string
	host       string
	weight     int64

	Conf *Config
)

func init() {
	var (
		defHost, _   = os.Hostname()
		defWeight, _ = strconv.ParseInt(os.Getenv("WEIGHT"), 10, 32)
	)
	flag.StringVar(&configPath, "config", "logic.toml", "default config path")

	flag.StringVar(&host, "host", defHost, "machine hostname. or use default machine hostname.")
	flag.Int64Var(&weight, "weight", defWeight, "load balancing weight, or use WEIGHT env variable, value: 10 etc.")

}

func Init() (err error) {
	Conf = NewConfig()
	_, err = toml.DecodeFile(configPath, &Conf)
	return
}

func NewConfig() *Config {
	return &Config{}
}

type Config struct {
}
