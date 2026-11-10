package http

import (
	"net/http"

	"grab-mini/profile-service/internal/app"
)

// listQuery reads GET /driver-applications?status=&limit=&cursor= as the raw strings: the
// use case checks them, in that order (an empty value is the default: PENDING, 50, from the
// start).
func listQuery(r *http.Request) app.ListQuery {
	q := r.URL.Query()
	return app.ListQuery{Status: q.Get("status"), Limit: q.Get("limit"), Cursor: q.Get("cursor")}
}
