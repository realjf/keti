package main

import (
	"flag"
	"os"

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
	s := app.NewServer()

	s.Init(port, dbConn)
	s.Start()
}
