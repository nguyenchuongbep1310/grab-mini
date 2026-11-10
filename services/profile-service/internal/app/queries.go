package app

import (
	"context"
	"errors"

	"grab-mini/profile-service/internal/domain"
)

// ListQuery carries the raw query-string values; the order of refusals is behaviour.
type ListQuery struct{ Status, Limit, Cursor string }

// Page is one page of the application queue. NextCursor is nil on the last page.
type Page struct {
	Drivers    []domain.Driver
	NextCursor *string
}

// GetDriver returns the driver's own profile; a driver who never saved one is NONE.
func (s *Service) GetDriver(ctx context.Context, id string) (domain.Driver, error) {
	d, err := s.drivers.Driver(ctx, id)
	if errors.Is(err, domain.ErrNoDriver) {
		return domain.Driver{ID: id, Status: domain.StatusNone}, nil
	}
	if err != nil {
		return d, err
	}
	return d, nil
}

// PublicDriver returns an APPROVED driver for the public view. A missing driver and any
// other status are the same domain.ErrNoDriver (counted not_found); a load error is returned
// as it is and counts nothing.
func (s *Service) PublicDriver(ctx context.Context, id string) (domain.Driver, error) {
	d, err := s.drivers.Driver(ctx, id)
	if err != nil && !errors.Is(err, domain.ErrNoDriver) {
		return domain.Driver{}, err
	}
	if err != nil || d.Status != domain.StatusApproved {
		s.metrics.PublicLookup("not_found")
		return domain.Driver{}, domain.ErrNoDriver
	}
	s.metrics.PublicLookup("found")
	return d, nil
}

// ListApplications returns the drivers with one status, the ones waiting longest first.
// Status, limit and cursor are checked in that order.
func (s *Service) ListApplications(ctx context.Context, q ListQuery) (Page, error) {
	status, err := domain.ParseStatusFilter(q.Status)
	if err != nil {
		return Page{}, err
	}
	limit, err := domain.ParseLimit(q.Limit)
	if err != nil {
		return Page{}, err
	}
	var after *domain.Cursor
	if q.Cursor != "" { // ParseCursor refuses ""; no cursor means from the start
		c, err := domain.ParseCursor(q.Cursor)
		if err != nil {
			return Page{}, err
		}
		after = &c
	}
	rows, err := s.queue.ByStatus(ctx, status, limit+1, after)
	if err != nil {
		return Page{}, err
	}
	var next *string
	if len(rows) > limit {
		rows = rows[:limit]
		c := domain.CursorFor(rows[limit-1])
		next = &c
	}
	if rows == nil {
		rows = []domain.Driver{}
	}
	return Page{Drivers: rows, NextCursor: next}, nil
}
