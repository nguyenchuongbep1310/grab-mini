package app

import (
	"context"
	"log"

	"grab-mini/profile-service/internal/domain"
)

// SaveRiderCommand carries the raw request values; cleaning is the use case's.
type SaveRiderCommand struct{ ID, Name, Phone string }

// GetRider returns the rider's profile; a rider without one gets an empty profile and no
// error, so an app does not have to treat the first visit as an error.
func (s *Service) GetRider(ctx context.Context, id string) (domain.Rider, error) {
	r, found, err := s.riders.Rider(ctx, id)
	if err != nil {
		return domain.Rider{}, err
	}
	if !found {
		return domain.Rider{ID: id}, nil
	}
	return r, nil
}

// SaveRider replaces the rider's name (required) and phone (optional). Name is cleaned
// before phone; the clock is read before the write; the counter and the log (ids only) come
// after it.
func (s *Service) SaveRider(ctx context.Context, c SaveRiderCommand) (domain.Rider, error) {
	name, ferr := domain.CleanName(c.Name)
	if ferr != nil {
		return domain.Rider{}, ferr
	}
	phone, ferr := domain.CleanPhone(c.Phone)
	if ferr != nil {
		return domain.Rider{}, ferr
	}
	r := domain.Rider{ID: c.ID, Name: name, Phone: phone, UpdatedAt: s.now()}
	if err := s.riders.SaveRider(ctx, r); err != nil {
		return r, err
	}
	s.metrics.Updated("rider")
	log.Printf("rider %s updated their profile", c.ID) // ids only
	return r, nil
}
