package helper

import "strings"

//Responseis used for static shape json return
type Response struct {
	Code    int64       `json:"code"`
	Message string      `json:"message"`
	Errors  interface{} `json:"errors"`
	Data    interface{} `json:"data"`
}

//EmptyObj object is used when data doesn't want to be null on json
type EmptyObj struct{}

func BuildResponse(code int64, message string, data interface{}) Response {
	res := Response{
		Code:    code,
		Message: message,
		Errors:  nil,
		Data:    data,
	}

	return res
}

func BuildErrorResponse(code int64, message string, err string, data interface{}) Response {
	splittedError := strings.Split(err, "\n")
	res := Response{
		Code:    code,
		Message: message,
		Errors:  splittedError,
		Data:    data,
	}

	return res
}
