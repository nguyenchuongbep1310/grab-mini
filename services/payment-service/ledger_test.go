package main

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"grab-mini/pkg/events"
)

func TestValidateAcceptsABalancedTransaction(t *testing.T) {
	txn := Txn{ID: "t", Key: "fare:1", Kind: "FARE", Currency: currencyVND, Entries: []entry{
		{riderAccount("r"), -100},
		{driverPayableAccount("d"), 80},
		{platformRevenueAccount(), 20},
	}}
	if err := txn.Validate(); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
}

func TestValidateRefusesAnUnbalancedTransactionAndCountsIt(t *testing.T) {
	before := testutil.ToFloat64(unbalancedRejected)
	txn := Txn{ID: "t", Key: "fare:1", Kind: "FARE", Currency: currencyVND, Entries: []entry{
		{riderAccount("r"), -100},
		{driverPayableAccount("d"), 80},
		{platformRevenueAccount(), 19}, // a dong lost
	}}
	err := txn.Validate()
	var ub *errUnbalanced
	if !errors.As(err, &ub) || !ub.Permanent() {
		t.Fatalf("Validate = %v, want a permanent errUnbalanced", err)
	}
	if got := testutil.ToFloat64(unbalancedRejected) - before; got != 1 {
		t.Errorf("ledger_unbalanced_rejected_total rose by %v, want 1", got)
	}
}

func TestValidateRefusesMalformedTransactions(t *testing.T) {
	r, d := riderAccount("r"), driverPayableAccount("d")
	cases := map[string]Txn{
		"one entry":     {ID: "t", Key: "k", Currency: currencyVND, Entries: []entry{{r, 0}}},
		"a zero entry":  {ID: "t", Key: "k", Currency: currencyVND, Entries: []entry{{r, -5}, {d, 5}, {platformRevenueAccount(), 0}}},
		"no key":        {ID: "t", Currency: currencyVND, Entries: []entry{{r, -5}, {d, 5}}},
		"no currency":   {ID: "t", Key: "k", Entries: []entry{{r, -5}, {d, 5}}},
		"out of bounds": {ID: "t", Key: "k", Currency: currencyVND, Entries: []entry{{r, -maxAmount - 1}, {d, maxAmount + 1}}},
		"empty":         {ID: "t", Key: "k", Currency: currencyVND},
		"no id":         {Key: "k", Currency: currencyVND, Entries: []entry{{r, -5}, {d, 5}}},
	}
	for name, txn := range cases {
		if err := txn.Validate(); err == nil {
			t.Errorf("%s: Validate = nil, want an error", name)
		}
	}
}

func TestChargeTxnAlwaysBalances(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 20000; i++ {
		amount := 1 + rng.Int63n(maxAmount)
		driver, platform := split(amount, rng.Int63n(bpsDenominator+1))
		txn := chargeTxn("t", "k", events.PaymentKindFare, "r", "d", driver, platform)
		if err := txn.Validate(); err != nil {
			t.Fatalf("amount %d split %d/%d: %v", amount, driver, platform, err)
		}
		for _, e := range txn.Entries {
			if e.amount == 0 {
				t.Fatalf("zero entry for %d split %d/%d", amount, driver, platform)
			}
		}
	}
}

func TestChargeTxnLeavesOutAZeroShare(t *testing.T) {
	tip := chargeTxn("t", "tip:1", events.PaymentKindTip, "r", "d", 20000, 0)
	if len(tip.Entries) != 2 {
		t.Fatalf("a tip has %d entries, want 2 (rider, driver)", len(tip.Entries))
	}
	all := chargeTxn("t", "fare:1", events.PaymentKindFare, "r", "d", 0, 5000)
	if len(all.Entries) != 2 || all.Entries[1].acct.kind != accountPlatformRevenue {
		t.Fatalf("a fare kept wholly by the platform has entries %+v", all.Entries)
	}
	for _, txn := range []Txn{tip, all} {
		if err := txn.Validate(); err != nil {
			t.Error(err)
		}
	}
}
