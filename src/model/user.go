package model

type User struct {
	ID       int    `gorm:"primaryKey;type:int;autoIncrement;not null" json:"id"`
	Name     string `gorm:"type:varchar(255);not null" json:"name"`
	Email    string `gorm:"uniqueIndex;type:varchar(255);not null" json:"email"`
	Password string `gorm:"type:string;not null;" json:"-"`
}
