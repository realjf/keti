package router

import (
	"github.com/gorilla/mux"
	"github.com/realjf/keti/pkg/routes"
	V1SubRoutes "github.com/realjf/keti/src/controller/v1/router"
	"gorm.io/gorm"
)

type Router struct {
	Router *mux.Router
}

func NewRouter() *Router {
	r := &Router{
		Router: mux.NewRouter().StrictSlash(true),
	}
	return r
}

func (r *Router) Init(db *gorm.DB) {

	r.Router.Use(Middleware)

	baseRoutes := GetRoutes(db)
	for _, route := range baseRoutes {
		r.Router.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}

	v1SubRoutes := V1SubRoutes.GetRoutes(db)
	for name, pack := range v1SubRoutes {
		r.AttachSubRouterWithMiddleware(name, pack.Routes, pack.Middleware)
	}
}

func (r *Router) AttachSubRouterWithMiddleware(path string, subroutes routes.Routes, middleware mux.MiddlewareFunc) (SubRouter *mux.Router) {
	SubRouter = r.Router.PathPrefix(path).Subrouter()
	SubRouter.Use(middleware)

	for _, route := range subroutes {
		SubRouter.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}

	return
}
