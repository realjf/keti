package app

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/handlers"
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

	handler := handlers.LoggingHandler(os.Stdout, handlers.CORS(
		handlers.AllowedOrigins([]string{"*"}),
		handlers.AllowedMethods([]string{"GET", "PUT", "PATCH", "POST", "DELETE", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Content-Type", "Origin", "Cache-Control", "X-App-Token"}),
		handlers.ExposedHeaders([]string{""}),
		handlers.MaxAge(1000),
		handlers.AllowCredentials(),
	)(r.Router))
	handler = handlers.RecoveryHandler(handlers.PrintRecoveryStack(true))(handler)

	newServer := &http.Server{
		Handler:      handler,
		Addr:         "0.0.0.0" + s.port,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}
	log.Fatal(newServer.ListenAndServe())
}
