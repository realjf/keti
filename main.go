package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"log"

	"github.com/joho/godotenv"
	"github.com/realjf/keti/src/app"
	"github.com/realjf/keti/src/db"
	"gorm.io/gorm"
)

var (
	port   string
	dbConn *gorm.DB
)

func init() {
	flag.StringVar(&port, "port", "8080", "Assigning the port that the server should listen on.")

	flag.Parse()

	if err := godotenv.Load("config.ini"); err != nil {
		panic(err)
	}

	dbConn = db.Connect()

	envPort := os.Getenv("PORT")
	if len(envPort) > 0 {
		port = envPort
	}
}

func main() {
	srv := app.NewServer()

	go func() {
		srv.Init(port, dbConn)
		srv.Start()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		log.Printf("keti get a signal %s\n", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:

			log.Println("keti exit")
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
