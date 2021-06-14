package app

import (
	"log"
	"net/http"

	"github.com/realjf/keti/src/router"
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

	r := router.NewRouter()
	r.Init()
	http.ListenAndServe(s.port, r.Router)
}
