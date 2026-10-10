package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/surge"
)

var (
	surgeMax = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pricing_surge_multiplier_max",
		Help: "Highest surge multiplier across cells at the last sample, by vehicle type.",
	}, []string{"vehicle_type"})
	surgeCells = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pricing_surging_cells",
		Help: "Cells with a multiplier above 1.0 at the last sample, by vehicle type.",
	}, []string{"vehicle_type"})
	sampleErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pricing_sample_errors_total",
		Help: "Cell samples that failed (Redis or supply lookup); the cell keeps its old multiplier until it expires.",
	})
	sampleTicks = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pricing_ticks_total",
		Help: "Sampling rounds this instance ran as the leader.",
	})
)

// supplyFunc counts the AVAILABLE drivers in a cell, by vehicle type.
type supplyFunc func(ctx context.Context, cell string) (map[string]int, error)

// redisSupply counts supply from the driver GEO set: the AVAILABLE drivers
// within the equal-area radius of the cell's centre (a circle with the area
// of the square cell, so neighbouring cells neither overlap much nor leave
// big gaps), split by vehicle. With GEO sharding on, this reads only the
// shard keys around that cell.
func redisSupply(rdb *redis.Client, deg float64) supplyFunc {
	radiusKm := deg * 111.32 / math.Sqrt(math.Pi)
	return func(ctx context.Context, cell string) (map[string]int, error) {
		lat, lng, err := surge.CellCenter(deg, cell)
		if err != nil {
			return nil, err
		}
		found, err := driverlocation.Search(ctx, rdb, lat, lng, radiusKm, 1000)
		if err != nil {
			return nil, err
		}
		ids := make([]string, len(found))
		for i, c := range found {
			ids[i] = c.ID
		}
		avail, err := driverstatus.AvailableVehicles(ctx, rdb, ids)
		if err != nil {
			return nil, err
		}
		out := map[string]int{}
		for _, v := range avail {
			out[v]++
		}
		return out, nil
	}
}

type sampler struct {
	rdb    *redis.Client
	env    Env
	supply supplyFunc
	now    func() time.Time
}

const lockKey = "surge:sampler:lock"

// run samples every env.SampleInterval until ctx is cancelled.
func (s *sampler) run(ctx context.Context) {
	t := time.NewTicker(s.env.SampleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.tick(ctx); err != nil {
				log.Printf("[%s] surge sample failed: %v", s.env.InstanceID, err)
			}
		}
	}
}

// tick refreshes the multiplier of every cell with recent demand. With
// several Pricing Service instances only the one holding a short lock
// samples: the smoothing in Smooth assumes one step per interval.
func (s *sampler) tick(ctx context.Context) error {
	lease := s.env.SampleInterval - time.Second
	if lease < time.Second {
		lease = time.Second
	}
	ok, err := s.rdb.SetNX(ctx, lockKey, s.env.InstanceID, lease).Result()
	if err != nil || !ok {
		return err
	}
	sampleTicks.Inc()

	members, err := s.rdb.SMembers(ctx, surge.CellsKey).Result()
	if err != nil {
		return err
	}
	now := s.now()
	supplyByCell := map[string]map[string]int{}
	maxByVehicle := map[string]float64{"CAR": 1, "BIKE": 1}
	surgingByVehicle := map[string]int{"CAR": 0, "BIKE": 0}

	for _, member := range members {
		vehicle, cell, ok := strings.Cut(member, "|")
		if !ok {
			s.rdb.SRem(ctx, surge.CellsKey, member)
			continue
		}
		next, err := s.refreshCell(ctx, vehicle, cell, now, supplyByCell)
		if err != nil {
			sampleErrors.Inc()
			log.Printf("[%s] surge sample %s: %v", s.env.InstanceID, member, err)
			continue
		}
		if next > maxByVehicle[vehicle] {
			maxByVehicle[vehicle] = next
		}
		if next > 1 {
			surgingByVehicle[vehicle]++
		}
	}
	for v, m := range maxByVehicle {
		surgeMax.WithLabelValues(v).Set(m)
		surgeCells.WithLabelValues(v).Set(float64(surgingByVehicle[v]))
	}
	return nil
}

// refreshCell computes and stores one cell's multiplier. A cell that has
// cooled off completely (no demand in the window and back at 1.0) is dropped.
func (s *sampler) refreshCell(ctx context.Context, vehicle, cell string, now time.Time, supplyByCell map[string]map[string]int) (float64, error) {
	demand, err := demandPerMin(ctx, s.rdb, vehicle, cell, s.env.WindowMinutes, now)
	if err != nil {
		return 0, err
	}
	supply, ok := supplyByCell[cell]
	if !ok {
		if supply, err = s.supply(ctx, cell); err != nil {
			return 0, err
		}
		supplyByCell[cell] = supply
	}

	prev := 1.0
	if v, err := s.rdb.Get(ctx, surge.MultKey(vehicle, cell)).Result(); err == nil {
		if f, perr := strconv.ParseFloat(v, 64); perr == nil {
			prev = f
		}
	} else if err != redis.Nil {
		return 0, err
	}

	target := surge.Multiplier(demand, float64(supply[vehicle]), s.env.K, s.env.MaxMultiplier)
	next := surge.Smooth(prev, target, s.env.Alpha, s.env.MaxStep, s.env.MaxMultiplier)

	if next <= 1.0 && demand == 0 {
		pipe := s.rdb.Pipeline()
		pipe.Del(ctx, surge.MultKey(vehicle, cell))
		pipe.SRem(ctx, surge.CellsKey, surge.CellMember(vehicle, cell))
		_, err := pipe.Exec(ctx)
		return 1.0, err
	}
	if err := s.rdb.Set(ctx, surge.MultKey(vehicle, cell), fmt.Sprintf("%.1f", next), surge.MultTTL).Err(); err != nil {
		return 0, err
	}
	return next, nil
}
