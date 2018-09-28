/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package mem provides the following derived metrics:
//	- memory.usage.percent
package mem // import "github.com/solnx/hurricane/internal/mem"

import (
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	wall "github.com/solnx/eye/lib/eye.wall"
)

// Mem implements the metric evaluation and accounting for monitoring
// of memory metrics
type Mem struct {
	assetID  int64
	curr     distribution
	next     distribution
	currTime time.Time
	nextTime time.Time
	usage    float64
	lookup   *wall.Lookup
	ack      []*erebos.Transport
}

// Update adds mtr to the next distribution tracked by Mem
func (m *Mem) update(mtr *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {
	// set assetID on first use
	if m.assetID == 0 {
		m.assetID = mtr.AssetID
	}

	// check update has correct assetID
	if m.assetID != mtr.AssetID {
		// send back t so the offset gets committed
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

processing:
	// first metric for this distribution
	if m.nextTime.IsZero() {
		m.nextTime = mtr.TS
	}

	// out of order metric for old timestamp
	if m.nextTime.After(mtr.TS) {
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

	// abandon current next and start new one
	if m.nextTime.Before(mtr.TS) {
		m.nextTime = time.Time{}
		m.next = distribution{}
		goto processing
	}

	if m.nextTime.Equal(mtr.TS) {
		switch mtr.Path {
		case `/sys/memory/active`:
			m.next.active = mtr.Value().(int64)
			m.next.setActive = true
		case `/sys/memory/buffers`:
			m.next.buffers = mtr.Value().(int64)
			m.next.setBuffers = true
		case `/sys/memory/cached`:
			m.next.cached = mtr.Value().(int64)
			m.next.setCached = true
		case `/sys/memory/free`:
			m.next.free = mtr.Value().(int64)
			m.next.setFree = true
		case `/sys/memory/inactive`:
			m.next.inactive = mtr.Value().(int64)
			m.next.setInactive = true
		case `/sys/memory/swapfree`:
			m.next.swapFree = mtr.Value().(int64)
			m.next.setSwapFree = true
		case `/sys/memory/swaptotal`:
			m.next.swapTotal = mtr.Value().(int64)
			m.next.setSwapTotal = true
		case `/sys/memory/total`:
			m.next.total = mtr.Value().(int64)
			m.next.setTotal = true
		}
	}

	m.ack = append(m.ack, t)
	return m.calculate()
}

// Calculate checks if the next distribution has been fully assembled
// and then calculates the memory usage, moves the distribution forward
// and returns the derived metric. If the distribution is not yet
// complete, it returns nil.
func (m *Mem) calculate() ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {

	if m.nextTime.IsZero() || !m.next.valid() {
		return nil, nil, false, nil
	}

	// do not walk backwards in time
	if m.currTime.After(m.nextTime) || m.currTime.Equal(m.nextTime) {
		return nil, nil, false, nil
	}

	usage := big.NewRat(0, 1).SetFrac64(m.next.free, m.next.total)
	usage.Mul(usage, big.NewRat(100, 1))
	usage.Sub(big.NewRat(100, 1), usage)
	m.usage, _ = strconv.ParseFloat(usage.FloatString(2), 64)
	m.usage = round(m.usage, .5, 2)

	m.nextToCurrent()
	derived, err := m.emitMetric()
	if err != nil {
		return nil, nil, false, err
	}
	acks := m.ack
	m.ack = []*erebos.Transport{}
	return derived, acks, true, nil
}

// nextToCurrent advances the distributions within Mem by one step
func (m *Mem) nextToCurrent() {
	m.currTime = m.nextTime
	m.nextTime = time.Time{}

	m.curr = m.next
	m.next = distribution{}
}

// emitMetric returns a legacy.MetricSplit for metric path
// memory.usage.percent with the m.Usage as value
func (m *Mem) emitMetric() ([]*legacy.MetricSplit, error) {
	mup := &legacy.MetricSplit{
		AssetID: m.assetID,
		Path:    `memory.usage.percent`,
		TS:      m.currTime,
		Type:    `real`,
		Unit:    `%`,
		Val: legacy.MetricValue{
			FlpVal: m.usage,
		},
	}
	if tags, err := m.lookup.GetConfigurationID(
		mup.LookupID(),
	); err == nil {
		mup.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	return []*legacy.MetricSplit{mup}, nil
}

// distribution is used to track multiple memory metrics from the same
// measurement cycle
type distribution struct {
	setTotal     bool
	setActive    bool
	setBuffers   bool
	setCached    bool
	setFree      bool
	setInactive  bool
	setSwapFree  bool
	setSwapTotal bool
	total        int64
	active       int64
	buffers      int64
	cached       int64
	free         int64
	inactive     int64
	swapFree     int64
	swapTotal    int64
}

// valid checks if a distribution has been fully populated
func (m *distribution) valid() bool {
	return m.setTotal && m.setActive && m.setBuffers && m.setCached &&
		m.setFree && m.setInactive && m.setSwapFree && m.setSwapTotal
}

// https://gist.github.com/DavidVaini/10308388
func round(val float64, roundOn float64, places int) (newVal float64) {
	var round float64
	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)
	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}
	newVal = round / pow
	return
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
