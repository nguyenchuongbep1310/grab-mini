// Package driverstatus holds the Redis key format and operations for a
// driver's status, shared by every service that reads or moves a
// driver between AVAILABLE / ASSIGNED / ON_TRIP.
package driverstatus

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Status string

const (
	Available Status = "AVAILABLE"
	Assigned  Status = "ASSIGNED"
	OnTrip    Status = "ON_TRIP"
)

func key(driverID string) string {
	return fmt.Sprintf("driver:%s:status", driverID)
}

// Set records a driver's status and ends any assignment lease (see
// ClaimForOrder): whoever calls Set has decided what the driver is doing now.
func Set(ctx context.Context, rdb *redis.Client, driverID string, status Status) error {
	_, err := rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, key(driverID), string(status), 0)
		p.Del(ctx, leaseKey(driverID))
		return nil
	})
	return err
}

func Get(ctx context.Context, rdb *redis.Client, driverID string) (Status, error) {
	v, err := rdb.Get(ctx, key(driverID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return Status(v), nil
}

func vehicleKey(driverID string) string {
	return fmt.Sprintf("driver:%s:vehicle", driverID)
}

// SetVehicle records what a driver drives (e.g. "CAR", "BIKE").
func SetVehicle(ctx context.Context, rdb *redis.Client, driverID, vehicle string) error {
	return rdb.Set(ctx, vehicleKey(driverID), vehicle, 0).Err()
}

// claimScript atomically moves a driver from expectedCurrent to newStatus,
// but only if they drive the wanted vehicle, so concurrent callers (e.g.
// two Matching Service instances) can never both succeed for the same driver.
var claimScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] and redis.call("GET", KEYS[2]) == ARGV[3] then
		redis.call("SET", KEYS[1], ARGV[2])
		return 1
	end
	return 0
`)

// Claim atomically moves a driver from expectedCurrent to newStatus if they
// drive the given vehicle. It reports whether this call performed the transition.
func Claim(ctx context.Context, rdb *redis.Client, driverID, vehicle string, expectedCurrent, newStatus Status) (bool, error) {
	claimed, err := claimScript.Run(ctx, rdb,
		[]string{key(driverID), vehicleKey(driverID)},
		string(expectedCurrent), string(newStatus), vehicle,
	).Int()
	if err != nil {
		return false, err
	}
	return claimed == 1, nil
}

func leaseKey(driverID string) string {
	return fmt.Sprintf("driver:%s:assigned_until", driverID)
}

func matchKey(orderID string) string {
	return fmt.Sprintf("match:%s", orderID)
}

// claimForOrderScript claims a driver for an order and records the decision,
// atomically, so a crash can't leave a driver claimed with no record of for
// which order. A driver is claimable when AVAILABLE, or when ASSIGNED with a
// lease that has run out (the claim was never followed by a trip, e.g. the
// matcher died before publishing). The lease clock is Redis's own TIME, so
// instances with different clocks agree.
//
// Returns 1 when claimed, 0 when this driver can't be claimed, and -1 when
// the order was already matched (the caller should read it with MatchedDriver).
var claimForOrderScript = redis.NewScript(`
	if redis.call("EXISTS", KEYS[4]) == 1 then
		return -1
	end
	local t = redis.call("TIME")
	local now = t[1] * 1000 + math.floor(t[2] / 1000)
	local status = redis.call("GET", KEYS[1])
	local free = (status == "AVAILABLE")
	if status == "ASSIGNED" then
		local leaseEnd = redis.call("GET", KEYS[3])
		if leaseEnd and tonumber(leaseEnd) <= now then
			free = true
		end
	end
	if (not free) or redis.call("GET", KEYS[2]) ~= ARGV[1] then
		return 0
	end
	redis.call("SET", KEYS[1], "ASSIGNED")
	redis.call("SET", KEYS[3], now + tonumber(ARGV[3]))
	redis.call("SET", KEYS[4], ARGV[2], "PX", ARGV[4])
	return 1
`)

// ClaimResult is the outcome of ClaimForOrder.
type ClaimResult int

const (
	NotClaimed     ClaimResult = 0
	Claimed        ClaimResult = 1
	AlreadyMatched ClaimResult = -1
)

// claimFirstForOrderScript is claimForOrderScript over a list of candidates,
// nearest first: it claims the first one that is claimable and returns its
// index (1-based), 0 when none is, and -1 when the order is already matched.
// Doing the walk inside Redis costs one round trip however many of the
// nearest drivers are busy, instead of one per busy driver.
//
// It builds the driver keys itself (driver:<id>:status etc.), so it can't run
// on Redis Cluster unless those keys are hash-tagged; see key().
var claimFirstForOrderScript = redis.NewScript(`
	if redis.call("EXISTS", KEYS[1]) == 1 then
		return -1
	end
	local t = redis.call("TIME")
	local now = t[1] * 1000 + math.floor(t[2] / 1000)
	for i = 5, #ARGV do
		local id = ARGV[i]
		local statusKey = "driver:" .. id .. ":status"
		local leaseKey = "driver:" .. id .. ":assigned_until"
		local status = redis.call("GET", statusKey)
		local free = (status == "AVAILABLE")
		if status == "ASSIGNED" then
			local leaseEnd = redis.call("GET", leaseKey)
			if leaseEnd and tonumber(leaseEnd) <= now then
				free = true
			end
		end
		if free and redis.call("GET", "driver:" .. id .. ":vehicle") == ARGV[1] then
			redis.call("SET", statusKey, "ASSIGNED")
			redis.call("SET", leaseKey, now + tonumber(ARGV[3]))
			redis.call("SET", KEYS[1], id, "PX", ARGV[4])
			return i - 4
		end
	end
	return 0
`)

// ClaimFirstForOrder claims the first claimable driver of candidates (in the
// order given) for orderID, like ClaimForOrder but in one round trip. It
// returns the claimed driver, or "" when none could be claimed; AlreadyMatched
// is returned when the order was matched before (read it with MatchedDriver).
func ClaimFirstForOrder(ctx context.Context, rdb *redis.Client, orderID string, candidates []string, vehicle string, lease time.Duration) (string, ClaimResult, error) {
	if len(candidates) == 0 {
		return "", NotClaimed, nil
	}
	args := []interface{}{vehicle, orderID, lease.Milliseconds(), MatchRecordTTL.Milliseconds()}
	for _, c := range candidates {
		args = append(args, c)
	}
	n, err := claimFirstForOrderScript.Run(ctx, rdb, []string{matchKey(orderID)}, args...).Int()
	if err != nil {
		return "", NotClaimed, err
	}
	switch {
	case n < 0:
		return "", AlreadyMatched, nil
	case n == 0:
		return "", NotClaimed, nil
	}
	return candidates[n-1], Claimed, nil
}

// MatchRecordTTL is how long the record of "order X went to driver Y" lives,
// which is how long a redelivered order.created is recognised.
const MatchRecordTTL = 24 * time.Hour

// ClaimForOrder claims driverID for orderID if they are claimable and drive
// the given vehicle, and records the match under the order. The driver stays
// ASSIGNED for at most lease: Trip Service ends the lease by moving them to
// ON_TRIP once the trip exists, and if that never happens the driver becomes
// claimable again instead of being stuck.
func ClaimForOrder(ctx context.Context, rdb *redis.Client, orderID, driverID, vehicle string, lease time.Duration) (ClaimResult, error) {
	n, err := claimForOrderScript.Run(ctx, rdb,
		[]string{key(driverID), vehicleKey(driverID), leaseKey(driverID), matchKey(orderID)},
		vehicle, driverID, lease.Milliseconds(), MatchRecordTTL.Milliseconds(),
	).Int()
	if err != nil {
		return NotClaimed, err
	}
	return ClaimResult(n), nil
}

// MatchedDriver returns the driver an order was matched to, or "" if the
// order has no recorded match.
func MatchedDriver(ctx context.Context, rdb *redis.Client, orderID string) (string, error) {
	v, err := rdb.Get(ctx, matchKey(orderID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

// ReleaseMatch gives a driver back and forgets the order's match, for when
// the match couldn't be announced and the order must be matched afresh.
func ReleaseMatch(ctx context.Context, rdb *redis.Client, orderID, driverID string) error {
	if err := Set(ctx, rdb, driverID, Available); err != nil {
		return err
	}
	return rdb.Del(ctx, matchKey(orderID)).Err()
}

// transitionScript moves a driver from one status to another only if they
// are in the first, and ends any assignment lease.
var transitionScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		redis.call("SET", KEYS[1], ARGV[2])
		redis.call("DEL", KEYS[2])
		return 1
	end
	return 0
`)

// Transition moves a driver from one status to another, only if they are in
// from right now, and reports whether it did. Unlike Set it never overwrites
// a status someone else changed in the meantime: releasing a driver who is
// ON_TRIP must not undo the claim of a new order that arrived after.
func Transition(ctx context.Context, rdb *redis.Client, driverID string, from, to Status) (bool, error) {
	n, err := transitionScript.Run(ctx, rdb,
		[]string{key(driverID), leaseKey(driverID)}, string(from), string(to),
	).Int()
	return n == 1, err
}
