package status

import "gorm.io/gorm"

var (
	DB *gorm.DB
)

func Init(db *gorm.DB) {
	DB = db
}
