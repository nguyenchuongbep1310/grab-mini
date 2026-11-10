package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"grab-mini/profile-service/internal/domain"
)

// SaveRider inserts or replaces the rider's name, phone and update time.
func (s *Store) SaveRider(ctx context.Context, r domain.Rider) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO rider_profiles (rider_id, name, phone, updated_at) VALUES ($1, $2, $3, $4)
		ON CONFLICT (rider_id) DO UPDATE SET name = EXCLUDED.name, phone = EXCLUDED.phone, updated_at = EXCLUDED.updated_at`,
		r.ID, r.Name, r.Phone, r.UpdatedAt)
	return err
}

// Rider reports false (and no error) when the rider has no row; the Rider then carries only
// the id.
func (s *Store) Rider(ctx context.Context, id string) (domain.Rider, bool, error) {
	r := domain.Rider{ID: id}
	err := s.db.QueryRow(ctx, `SELECT name, phone, updated_at FROM rider_profiles WHERE rider_id = $1`, id).Scan(&r.Name, &r.Phone, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	r.UpdatedAt = r.UpdatedAt.UTC()
	return r, err == nil, err
}
