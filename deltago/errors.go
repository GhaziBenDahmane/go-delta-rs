package deltago

import (
	"errors"
	"strings"

	"google.golang.org/grpc/status"
)

// DeltaError is a structured error returned by the delta-rs sidecar.
type DeltaError struct {
	Phase           string
	Code            string
	TableURI        string
	Message         string
	Retryable       bool
	AmbiguousCommit bool

	err error
}

func (e *DeltaError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.err != nil {
		return e.err.Error()
	}
	return "delta error"
}

func (e *DeltaError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// AsDeltaError returns the structured DeltaError contained in err, if any.
func AsDeltaError(err error) (*DeltaError, bool) {
	var deltaErr *DeltaError
	if errors.As(err, &deltaErr) {
		return deltaErr, true
	}
	return nil, false
}

func wrapDeltaError(err error) error {
	if err == nil {
		return nil
	}
	msg := status.Convert(err).Message()
	if !strings.HasPrefix(msg, "delta_error ") {
		return err
	}
	deltaErr := parseDeltaStatus(msg)
	deltaErr.err = err
	return deltaErr
}

func parseDeltaStatus(msg string) *DeltaError {
	body := strings.TrimPrefix(msg, "delta_error ")
	message := ""
	if idx := strings.Index(body, " message="); idx >= 0 {
		message = body[idx+len(" message="):]
		body = body[:idx]
	}

	fields := map[string]string{}
	for _, part := range strings.Fields(body) {
		key, value, ok := strings.Cut(part, "=")
		if ok {
			fields[key] = value
		}
	}

	retryable := fields["retryable"] == "true"
	ambiguous := fields["ambiguous"] == "true"
	if !ambiguous && fields["phase"] == "commit" && retryable {
		ambiguous = true
	}

	return &DeltaError{
		Phase:           fields["phase"],
		Code:            fields["code"],
		TableURI:        fields["table_uri"],
		Message:         message,
		Retryable:       retryable,
		AmbiguousCommit: ambiguous,
	}
}
