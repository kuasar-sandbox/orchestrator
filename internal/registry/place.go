package registry

import "context"

type noPlacer struct{}

func (noPlacer) Place(context.Context, PlaceRequest) (*Placement, error) {
	return nil, ErrNoNode
}
