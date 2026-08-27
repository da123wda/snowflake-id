package mutex

import "errors"

var (
	ErrInvalidBitLayout  = errors.New("snowflake-id/mutex: timestamp, machine, business and sequence bits must total 63; sequence needs at least 6 bits")
	ErrInvalidMachineID  = errors.New("snowflake-id/mutex: machine ID exceeds configured bit width")
	ErrInvalidBusinessID = errors.New("snowflake-id/mutex: business ID exceeds configured bit width")
	ErrInvalidTimestamp  = errors.New("snowflake-id/mutex: invalid timestamp")
	ErrInvalidID         = errors.New("snowflake-id/mutex: invalid ID")

	errInvalidSegmentSize = errors.New("snowflake-id/mutex: invalid segment size")
)
