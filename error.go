package rotor

import "errors"

// error
var (
	ErrInput            = errors.New("rotor:ErrInput")
	ErrBatchGetPage     = errors.New("rotor:ErrBatchGetPage")
	ErrItemNotFound     = errors.New("rotor:ErrItemNotFound")
	ErrConditionalCheck = errors.New("rotor:ErrConditionalCheck")
	ErrReturnValue      = errors.New("rotor:ErrReturnValue")
)
