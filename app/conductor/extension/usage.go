package extension

import "fmt"

// UsageQuery selects existing native state. Cursor is a native byte offset,
// serialized as a decimal string; reading never samples or saves usage.
type UsageQuery struct {
	View   string `json:"view,omitempty"`
	Cursor int64  `json:"cursor,omitempty,string"`
	Limit  int    `json:"limit,omitempty"`
}

func (q UsageQuery) Normalize() (UsageQuery, error) {
	if q.View == "" {
		q.View = "current"
	}
	if q.View != "current" && q.View != "saved" && q.View != "history" {
		return UsageQuery{}, fmt.Errorf("usage view must be current, saved or history")
	}
	if q.Cursor < 0 || q.Limit < 0 || q.Limit > 100 || (q.View != "history" && (q.Cursor != 0 || q.Limit != 0)) {
		return UsageQuery{}, fmt.Errorf("usage cursor/limit require history; cursor must be nonnegative and limit at most 100")
	}
	if q.View == "history" && q.Limit == 0 {
		q.Limit = 10
	}
	return q, nil
}
