package service

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	CodeInvalidArgument    = "INVALID_ARGUMENT"
	CodeNotFound           = "NOT_FOUND"
	CodeConflict           = "VERSION_CONFLICT"
	CodeAlreadyExists      = "ALREADY_EXISTS"
	CodeFailedPrecondition = "FAILED_PRECONDITION"
	CodeUnauthorized       = "UNAUTHORIZED"
	CodeUnavailable        = "UNAVAILABLE"
	CodeInternal           = "INTERNAL"
)

// Error is a stable service error suitable for MCP and HTTP clients.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
	Cause   error          `json:"-"`
}

// Error implements error.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if len(e.Details) > 0 {
		details, err := json.Marshal(e.Details)
		if err == nil {
			return fmt.Sprintf("%s: %s; details=%s", e.Code, e.Message, details)
		}
	}

	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap exposes the underlying error to errors.Is and errors.As.
func (e *Error) Unwrap() error {
	return e.Cause
}

// NewError creates a service error without an underlying cause.
func NewError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// WrapError creates a service error retaining the underlying cause.
func WrapError(code, message string, cause error) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// ErrorCode returns the stable code for a service error.
func ErrorCode(err error) string {
	var serviceErr *Error
	if errors.As(err, &serviceErr) {
		return serviceErr.Code
	}

	return CodeInternal
}
