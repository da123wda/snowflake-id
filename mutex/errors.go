package mutex

import "errors"

var (
	ErrInvalidMachineID = errors.New("snowflake-id/mutex: machine ID must be between 0 and 1023")
	ErrInvalidTimestamp = errors.New("snowflake-id/mutex: invalid timestamp")
	ErrInvalidID        = errors.New("snowflake-id/mutex: invalid ID")

	errInvalidSegmentSize = errors.New("snowflake-id/mutex: segment size must be between 1 and 4096")
)
