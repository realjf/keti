package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	log "github.com/golang/glog"
)

var (
	ver = "1.0.0"
)

func main() {
	flag.Parse()
	log.Infof("keti-logic [version: %s env: %+v] start", ver)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		log.Infof("keti-logic get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:

			log.Infof("keti-logic [version: %s] exit", ver)
			log.Flush()
			return
		case syscall.SIGHUP:
		default:
			return
		}

	}
}
