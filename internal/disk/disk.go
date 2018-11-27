/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package disk provides the following derived metrics:
//	- disk.write.per.second
//	- disk.read.per.second
//	- disk.free
//	- disk.usage.percent
package disk // import "github.com/solnx/hurricane/internal/disk"

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/mjolnir42/erebos"
	wall "github.com/solnx/eye/lib/eye.wall"
	"github.com/solnx/legacy"
)

// dsk implements the logic to compute derived disk metrics
type dsk struct {
	assetID    int64
	curr       distribution
	next       distribution
	currTime   time.Time
	nextTime   time.Time
	mountpoint string
	readBps    float64
	writeBps   float64
	usage      float64
	bytesFree  int64
	lookup     *wall.Lookup
	ack        []*erebos.Transport
}

// Update adds m to the next counter tracked by d
func (d *dsk) update(m *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {
	// set assetID and mountpoint on first use
	if d.assetID == 0 {
		d.assetID = m.AssetID
	}
	if d.mountpoint == `` {
		d.mountpoint = m.Tags[0]
	}

	// check update has correct assetID and mountpoint
	if d.assetID != m.AssetID {
		// send back t so the offset gets committed
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}
	if d.mountpoint != m.Tags[0] {
		// send back t so the offset gets committed
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

processing:
	if d.nextTime.IsZero() {
		d.nextTime = m.TS
	}

	// out of order metric for old timestamp
	if d.nextTime.After(m.TS) {
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

	// abandon current next and start new one
	if d.nextTime.Before(m.TS) {
		d.nextTime = time.Time{}
		d.next = distribution{}
		goto processing
	}

	if d.nextTime.Equal(m.TS) {
		switch m.Path {
		case `/sys/disk/blk_total`:
			d.next.blkTotal = m.Value().(int64) * 1024
			d.next.setBlkTotal = true
		case `/sys/disk/blk_used`:
			d.next.blkUsed = m.Value().(int64) * 1024
			d.next.setBlkUsed = true
		case `/sys/disk/blk_read`:
			d.next.blkRead = m.Value().(int64) * 512
			d.next.setBlkRead = true
		case `/sys/disk/blk_wrtn`:
			d.next.blkWrite = m.Value().(int64) * 512
			d.next.setBlkWrite = true
		}
	}

	d.ack = append(d.ack, t)
	return d.calculate()
}

// calculate checks if the next counter has been fully assembled and
// then calculates the derived metrics, moves the counters forward and
// returns the derived metrics. If the next counter is not yet complete,
// it returns nil.
func (d *dsk) calculate() ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {

	if d.nextTime.IsZero() || !d.next.valid() {
		return nil, nil, false, nil
	}

	// do not walk backwards in time
	if d.currTime.After(d.nextTime) || d.currTime.Equal(d.nextTime) {
		return nil, nil, false, nil
	}

	usage := big.NewRat(0, 1).SetFrac64(
		d.next.blkUsed,
		d.next.blkTotal,
	)
	usage.Mul(usage, big.NewRat(100, 1))
	floatUsage, _ := strconv.ParseFloat(usage.FloatString(2), 64)
	floatUsage = round(floatUsage, .5, 2)

	bytesFree := d.next.blkTotal - d.next.blkUsed

	d.usage = floatUsage
	d.bytesFree = bytesFree

	// this is the first update
	if d.currTime.IsZero() {
		d.nextToCurrent()
		return nil, nil, false, nil
	}

	delta := d.nextTime.Sub(d.currTime).Seconds()

	reads := d.next.blkRead - d.curr.blkRead
	writes := d.next.blkWrite - d.curr.blkWrite

	// counter wrapped
	if reads < 0 || writes < 0 {
		d.nextToCurrent()
		return nil, nil, false, nil
	}

	d.readBps = float64(reads) / delta
	d.readBps = round(d.readBps, .5, 2)
	d.writeBps = float64(writes) / delta
	d.writeBps = round(d.writeBps, .5, 2)

	d.nextToCurrent()
	derived, err := d.emitMetric()
	if err != nil {
		return nil, nil, false, err
	}
	acks := d.ack
	d.ack = []*erebos.Transport{}
	return derived, acks, true, nil
}

// nextToCurrent advances the counters within d by one step
func (d *dsk) nextToCurrent() {
	d.currTime = d.nextTime
	d.nextTime = time.Time{}

	d.curr = d.next
	d.next = distribution{}
}

// emitMetric returns the derived metrics for the current counter
func (d *dsk) emitMetric() ([]*legacy.MetricSplit, error) {
	dwps := &legacy.MetricSplit{
		AssetID: d.assetID,
		Path: fmt.Sprintf("disk.write.per.second:%s",
			d.mountpoint),
		TS:   d.currTime,
		Type: `real`,
		Unit: `B`,
		Val: legacy.MetricValue{
			FlpVal: d.writeBps,
		},
	}
	if tags, err := d.lookup.GetConfigurationID(
		dwps.LookupID(),
	); err == nil {
		dwps.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	drps := &legacy.MetricSplit{
		AssetID: d.assetID,
		Path: fmt.Sprintf("disk.read.per.second:%s",
			d.mountpoint),
		TS:   d.currTime,
		Type: `real`,
		Unit: `B`,
		Val: legacy.MetricValue{
			FlpVal: d.readBps,
		},
	}
	if tags, err := d.lookup.GetConfigurationID(
		drps.LookupID(),
	); err == nil {
		drps.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	df := &legacy.MetricSplit{
		AssetID: d.assetID,
		Path: fmt.Sprintf("disk.free:%s",
			d.mountpoint),
		TS:   d.currTime,
		Type: `integer`,
		Unit: `B`,
		Val: legacy.MetricValue{
			IntVal: d.bytesFree,
		},
	}
	if tags, err := d.lookup.GetConfigurationID(
		df.LookupID(),
	); err == nil {
		df.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	dup := &legacy.MetricSplit{
		AssetID: d.assetID,
		Path: fmt.Sprintf("disk.usage.percent:%s",
			d.mountpoint),
		TS:   d.currTime,
		Type: `real`,
		Unit: `%`,
		Val: legacy.MetricValue{
			FlpVal: d.usage,
		},
	}
	if tags, err := d.lookup.GetConfigurationID(
		dup.LookupID(),
	); err == nil {
		dup.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	return []*legacy.MetricSplit{dwps, drps, df, dup}, nil
}

// distribution is used to track multiple disk metrics from the same
// measurement cycle
type distribution struct {
	setBlkTotal bool
	setBlkUsed  bool
	setBlkRead  bool
	setBlkWrite bool
	blkTotal    int64
	blkUsed     int64
	blkRead     int64
	blkWrite    int64
}

// valid checks if a counter has been fully populated
func (d *distribution) valid() bool {
	return d.setBlkTotal && d.setBlkUsed && d.setBlkRead && d.setBlkWrite
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
