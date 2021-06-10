package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/realjf/keti/internal/logic/config"
	"github.com/realjf/keti/pkg/utils"
)

var (
	log = utils.Logger
	ver = "1.0.0"
)

func main() {
	flag.Parse()
	err := config.Init()
	if err != nil {
		panic(err)
	}
	log.Infof("keti-logic [version: %s] start", ver)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		log.Infof("keti-logic get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:

			log.Infof("keti-logic [version: %s] exit", ver)
			return
		case syscall.SIGHUP:
		default:
			return
		}

	}
}
