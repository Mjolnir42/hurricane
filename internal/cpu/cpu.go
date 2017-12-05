/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package cpu provides the following derived metrics:
//	- cpu.usage.percent
package cpu // import "github.com/mjolnir42/hurricane/internal/cpu"

import (
	"math"
	"time"

	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/eyewall"
	"github.com/mjolnir42/legacy"
)

// CPU implements the logic to compute derived cpu usage metrics
type CPU struct {
	assetID  int64
	curr     distribution
	next     distribution
	currTime time.Time
	nextTime time.Time
	idle     int64
	nonIdle  int64
	total    int64
	usage    float64
	lookup   *eyewall.Lookup
	ack      []*erebos.Transport
}

// Update adds m to the next counter tracked by c
func (c *CPU) update(m *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {
	// set assetID on first use
	if c.assetID == 0 {
		c.assetID = m.AssetID
	}

	// check update has correct assetID
	if c.assetID != m.AssetID {
		// send back t so the offset gets committed
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

	// only process metrics tagged as cpu, not cpuN
	cpuMetric := false
	for _, tag := range m.Tags {
		if tag == `cpu` {
			cpuMetric = true
			break
		}
	}
	if !cpuMetric {
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

processing:
	if c.nextTime.IsZero() {
		c.nextTime = m.TS
	}

	// out of order metric for old timestamp
	if c.nextTime.After(m.TS) {
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

	// abandon current next and start new one
	if c.nextTime.Before(m.TS) {
		c.nextTime = time.Time{}
		c.next = distribution{}
		goto processing
	}

	if c.nextTime.Equal(m.TS) {
		switch m.Path {
		case `/sys/cpu/count/idle`:
			c.next.idle = m.Value().(int64)
			c.next.setIdle = true
		case `/sys/cpu/count/iowait`:
			c.next.ioWait = m.Value().(int64)
			c.next.setIoWait = true
		case `/sys/cpu/count/irq`:
			c.next.irq = m.Value().(int64)
			c.next.setIrq = true
		case `/sys/cpu/count/nice`:
			c.next.nice = m.Value().(int64)
			c.next.setNice = true
		case `/sys/cpu/count/softirq`:
			c.next.softIrq = m.Value().(int64)
			c.next.setSoftIrq = true
		case `/sys/cpu/count/system`:
			c.next.system = m.Value().(int64)
			c.next.setSystem = true
		case `/sys/cpu/count/user`:
			c.next.user = m.Value().(int64)
			c.next.setUser = true
		}
	}

	c.ack = append(c.ack, t)
	return c.calculate()
}

// calculate checks if the next counter has been fully assembled and
// then calculates the derived metrics, moves the counters forward and
// returns he derived metrics. If the next counter is not yet complete,
// it returns nil.
func (c *CPU) calculate() ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {

	if c.nextTime.IsZero() || !c.next.valid() {
		return nil, nil, false, nil
	}

	// do not walk backwards in time
	if c.currTime.After(c.nextTime) || c.currTime.Equal(c.nextTime) {
		return nil, nil, false, nil
	}

	nextIdle := c.next.idle + c.next.ioWait
	nextNonIdle := c.next.user + c.next.nice + c.next.system +
		c.next.irq + c.next.softIrq

	// this is the first update
	if c.currTime.IsZero() {
		c.idle = nextIdle
		c.nonIdle = nextNonIdle
		c.total = nextIdle + nextNonIdle

		c.nextToCurrent()
		return nil, nil, false, nil
	}

	totalDifference := (nextIdle + nextNonIdle) - c.total
	idleDifference := nextIdle - c.idle
	c.usage = float64((totalDifference - idleDifference)) / float64(totalDifference)
	c.usage = round(c.usage, .5, 4) * 100

	c.idle = nextIdle
	c.nonIdle = nextNonIdle
	c.total = nextIdle + nextNonIdle

	c.nextToCurrent()
	derived, err := c.emitMetric()
	if err != nil {
		return nil, nil, false, err
	}
	acks := c.ack
	c.ack = []*erebos.Transport{}
	return derived, acks, true, nil
}

// nextToCurrent advances the counters within c by one step
func (c *CPU) nextToCurrent() {
	c.currTime = c.nextTime
	c.nextTime = time.Time{}

	c.curr = c.next
	c.next = distribution{}
}

// emitMetric returns the derived metrics for the current counter
func (c *CPU) emitMetric() ([]*legacy.MetricSplit, error) {
	cup := &legacy.MetricSplit{
		AssetID: c.assetID,
		Path:    `cpu.usage.percent`,
		TS:      c.currTime,
		Type:    `real`,
		Unit:    `%`,
		Val: legacy.MetricValue{
			FlpVal: c.usage,
		},
	}
	if tag, err := c.lookup.GetConfigurationID(
		cup.LookupID(),
		cup.Path,
	); err == nil {
		cup.Tags = []string{tag}
	} else if err != eyewall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}
	return []*legacy.MetricSplit{cup}, nil
}

// distribution is used to track multiple cpu metrics from the same
// measurement cycle
type distribution struct {
	setIdle    bool
	setIoWait  bool
	setIrq     bool
	setNice    bool
	setSoftIrq bool
	setSystem  bool
	setUser    bool
	idle       int64
	ioWait     int64
	irq        int64
	nice       int64
	softIrq    int64
	system     int64
	user       int64
}

// valid checks if a counter has been fully populated
func (d *distribution) valid() bool {
	return d.setIdle && d.setIoWait && d.setIrq && d.setNice &&
		d.setSoftIrq && d.setSystem && d.setUser
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
