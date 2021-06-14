package repository

import (
	"log"

	"github.com/realjf/keti/src/model"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type UserRepository interface {
	InsertUser(user model.User) model.User
	UpdateUser(user model.User) model.User
	FindByEmail(email string) model.User
}

type userRepository struct {
	conn *gorm.DB
}

func NewUserRepository(db *gorm.DB) *userRepository {
	return &userRepository{
		conn: db,
	}
}

func (u *userRepository) InsertUser(user model.User) model.User {
	user.Password = hashAndSalt([]byte(user.Password))
	u.conn.Save(&user)
	return user
}

func (u *userRepository) UpdateUser(user model.User) model.User {
	if user.Password != "" {
		user.Password = hashAndSalt([]byte(user.Password))
	} else {
		var tempUser model.User
		u.conn.Find(&tempUser, user.ID)
		user.Password = tempUser.Password
	}
	u.conn.Save(&user)
	return user
}

func (u *userRepository) FindByEmail(email string) model.User {
	var user model.User
	u.conn.Where("email = ?", email).Take(&user)
	return user
}

func hashAndSalt(pwd []byte) string {
	hash, err := bcrypt.GenerateFromPassword(pwd, bcrypt.MinCost)
	if err != nil {
		log.Println(err)
		panic("Failed to hash a password")
	}
	return string(hash)
}
