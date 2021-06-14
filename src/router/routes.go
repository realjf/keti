package router

import (
	"net/http"

	"github.com/realjf/keti/pkg/routes"
	HomeHandler "github.com/realjf/keti/src/controller/home"
	"gorm.io/gorm"
)

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}

func GetRoutes(db *gorm.DB) routes.Routes {

	HomeHandler.Init(db)

	return routes.Routes{
		routes.Route{"Home", "GET", "/", HomeHandler.Index},
	}
}
