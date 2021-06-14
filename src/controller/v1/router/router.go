package router

import (
	"net/http"

	"github.com/realjf/keti/pkg/routes"
	StatusHandler "github.com/realjf/keti/src/controller/v1/status"
	"gorm.io/gorm"
)

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("X-App-Token")
		if len(token) < 1 {
			http.Error(w, "Not Authorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func GetRoutes(db *gorm.DB) (SubRoute map[string]routes.SubRoutePackage) {

	StatusHandler.Init(db)

	SubRoute = map[string]routes.SubRoutePackage{
		"/v1": routes.SubRoutePackage{
			Routes: routes.Routes{
				routes.Route{"Status", "GET", "/status", StatusHandler.Index},
			},
			Middleware: Middleware,
		},
	}

	return
}
