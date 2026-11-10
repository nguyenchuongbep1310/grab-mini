package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/profile-service/internal/domain"
)

const driverColumns = `driver_id, name, phone, vehicle_type, plate, model, color, status, reason, reviewed_by, reviewed_at, status_at, updated_at`

type scanner interface{ Scan(dest ...any) error }

// scanDriver reads a driver_profiles row; every timestamp comes back in UTC.
func scanDriver(row scanner) (domain.Driver, error) {
	var d domain.Driver
	var vehicle string
	err := row.Scan(&d.ID, &d.Name, &d.Phone, &vehicle, &d.Plate, &d.Model, &d.Color, &d.Status, &d.Reason, &d.ReviewedBy, &d.ReviewedAt, &d.StatusAt, &d.UpdatedAt)
	d.Vehicle = domain.VehicleType(vehicle)
	if d.ReviewedAt != nil {
		t := d.ReviewedAt.UTC()
		d.ReviewedAt = &t
	}
	d.StatusAt, d.UpdatedAt = d.StatusAt.UTC(), d.UpdatedAt.UTC()
	return d, err
}

type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// loadDriver reads a profile; forUpdate locks the row until the transaction ends, so two
// changes to one driver are applied one after the other.
func loadDriver(ctx context.Context, q queryer, id string, forUpdate bool) (domain.Driver, error) {
	sql := `SELECT ` + driverColumns + ` FROM driver_profiles WHERE driver_id = $1`
	if forUpdate {
		sql += ` FOR UPDATE`
	}
	d, err := scanDriver(q.QueryRow(ctx, sql, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return d, domain.ErrNoDriver
	}
	return d, err
}

// Store reads and writes profiles outside the driver transactions. It implements
// app.RiderStore, app.DriverReader, app.ApplicationQueue and app.ResyncSource.
type Store struct {
	db *pgxpool.Pool
}

// NewStore returns a Store over db.
func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

// Driver returns domain.ErrNoDriver when there is no row.
func (s *Store) Driver(ctx context.Context, id string) (domain.Driver, error) {
	return loadDriver(ctx, s.db, id, false)
}

// ByStatus returns up to limit drivers with the given status, oldest status change first,
// after the cursor position (nil: from the start).
func (s *Store) ByStatus(ctx context.Context, status string, limit int, after *domain.Cursor) ([]domain.Driver, error) {
	args := []any{status, limit}
	where := ""
	if after != nil {
		where = ` AND (status_at, driver_id) > ($3, $4)`
		args = append(args, after.At, after.ID)
	}
	rows, err := s.db.Query(ctx, `
		SELECT `+driverColumns+` FROM driver_profiles WHERE status = $1`+where+`
		ORDER BY status_at, driver_id LIMIT $2`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Driver
	for rows.Next() {
		d, err := scanDriver(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ResyncableIDs is the next batch of driver ids after the given one, in id order.
func (s *Store) ResyncableIDs(ctx context.Context, after string, limit int) ([]string, error) {
	rows, err := s.db.Query(ctx, `
		SELECT driver_id FROM driver_profiles
		WHERE status IN ('APPROVED', 'REJECTED', 'SUSPENDED') AND driver_id > $1
		ORDER BY driver_id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
