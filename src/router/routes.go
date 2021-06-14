package router

import (
	"net/http"

	"github.com/realjf/keti/pkg/routes"
	"github.com/realjf/keti/src/controller"
)

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}

func GetRoutes() routes.Routes {
	return routes.Routes{
		routes.Route{"Home", "GET", "/", controller.Index},
	}
}
