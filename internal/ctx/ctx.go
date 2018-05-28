/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package ctx provides the following derived metrics:
//	- ctx.per.second
package ctx // import "github.com/mjolnir42/hurricane/internal/ctx"

import (
	"math"
	"time"

	"github.com/mjolnir42/erebos"
	wall "github.com/mjolnir42/eye/lib/eye.wall"
	"github.com/mjolnir42/legacy"
)

// CTX implements the logic to compute derived context switch metrics
type CTX struct {
	assetID   int64
	currValue int64
	nextValue int64
	cps       float64
	currTime  time.Time
	nextTime  time.Time
	lookup    *wall.Lookup
	ack       []*erebos.Transport
}

// Update adds m to the next counter tracked by c and returns the
// derived metric if there is a new derived metric to be computed.
// Otherwise it returns nil.
func (c *CTX) update(m *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {
	// set assetID on first use
	if c.assetID == 0 {
		c.assetID = m.AssetID
	}

	// check update has correct assetID
	if c.assetID != m.AssetID {
		// send back t so the offset gets committed
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

	// first use, store values and transport
	if c.currTime.IsZero() {
		c.currTime = m.TS
		c.currValue = m.Value().(int64)
		c.ack = []*erebos.Transport{t}
		return nil, nil, false, nil
	}

	// backwards in time
	if c.currTime.After(m.TS) || c.currTime.Equal(m.TS) {
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

	c.nextTime = m.TS
	c.nextValue = m.Value().(int64)
	c.ack = append(c.ack, t)
	return c.calculate()
}

// calculate computes the derived metric between the current and next
// context switch counter
func (c *CTX) calculate() ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {
	ctx := c.nextValue - c.currValue
	delta := c.nextTime.Sub(c.currTime).Seconds()

	c.cps = float64(ctx) / delta
	c.cps = round(c.cps, .5, 2)

	c.nextToCurrent()
	derived, err := c.emitMetric()
	if err != nil {
		return nil, nil, false, err
	}
	acks := c.ack
	c.ack = []*erebos.Transport{}
	return derived, acks, true, nil
}

// nextToCurrent advances the measurement cycle within d by one step
func (c *CTX) nextToCurrent() {
	c.currValue = c.nextValue
	c.currTime = c.nextTime
	c.nextValue = 0
	c.nextTime = time.Time{}
}

// emitMetric returns the derived metrics for the current measurement
// cycle
func (c *CTX) emitMetric() ([]*legacy.MetricSplit, error) {
	cps := &legacy.MetricSplit{
		AssetID: c.assetID,
		Path:    `ctx.per.second`,
		TS:      c.currTime,
		Type:    `real`,
		Unit:    `#`,
		Val: legacy.MetricValue{
			FlpVal: c.cps,
		},
	}
	if tags, err := c.lookup.GetConfigurationID(
		cps.LookupID(),
	); err == nil {
		cps.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	return []*legacy.MetricSplit{cps}, nil
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
