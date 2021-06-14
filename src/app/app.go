package app

import (
	"log"
	"net/http"

	"gorm.io/gorm"
)

type Server struct {
	port string
	db   *gorm.DB
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Init(port string, db *gorm.DB) {
	log.Println("init server")
	s.port = ":" + port
	s.db = db
}

func (s *Server) Start() {
	log.Println("starting server on port " + s.port)
	http.ListenAndServe(s.port, nil)
}
