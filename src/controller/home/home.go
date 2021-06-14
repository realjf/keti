package home

import (
	"net/http"

	"gorm.io/gorm"
)

var (
	DB *gorm.DB
)

func Init(db *gorm.DB) {
	DB = db
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {

}
