package types

import "errors"

var ErrCheckpointLimit = errors.New("max_checkpoints reached; export or delete a retained checkpoint first")

func ResolveMaxCheckpoints(value *int) (int, error) {
	if value == nil {
		return 1, nil
	}
	if *value <= 0 {
		return 0, errors.New("max_checkpoints must be a positive integer")
	}
	return *value, nil
}
