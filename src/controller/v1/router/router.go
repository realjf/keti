package router

import (
	"log"
	"net/http"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/realjf/keti/pkg/routes"
	StatusHandler "github.com/realjf/keti/src/controller/v1/status"
	"github.com/realjf/keti/src/service"
	"gorm.io/gorm"
)

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("X-App-Token")
		if len(authHeader) < 1 {
			http.Error(w, "Not Authorized", http.StatusUnauthorized)
			return
		}

		jwtService := service.NewJWTService()
		token, err := jwtService.ValidateToken(authHeader)
		if err != nil {
			log.Println(err)
			http.Error(w, "Token is not valid", http.StatusUnauthorized)
			return
		}
		if token.Valid {
			claims := token.Claims.(jwt.MapClaims)
			log.Println("Claim[user_id]: ", claims["user_id"])
			log.Println("Claim[issuer]: ", claims["issuer"])
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
