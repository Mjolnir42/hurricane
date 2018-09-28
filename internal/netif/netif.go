/*-
 * Copyright © 2018, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package netif provides the following derived metrics:
//	- net.rx.average.packet.size.bytes:%dev
//	- net.rx.bandwidth.utilization.percent:%dev
//	- net.rx.bytes.per.second:%dev
//	- net.rx.packet.rate.utilization.percent:%dev
//	- net.rx.packets.per.second:%dev
//	- net.tx.average.packet.size.bytes:%dev
//	- net.tx.bandwidth.utilization.percent:%dev
//	- net.tx.bytes.per.second:%dev
//	- net.tx.packet.rate.utilization.percent:%dev
//	- net.tx.packets.per.second:%dev
//	- net.utilization.percent:%dev
package netif // import "github.com/solnx/hurricane/internal/netif"

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	wall "github.com/solnx/eye/lib/eye.wall"
)

// deviceAttributes contains Ethernet characteristics needed for
// utilization calculation
var deviceAttributes map[int64]map[string]int64

const (
	// maximum frame per second constants for differrent Ethernet types
	maxFPS10MbE  = 14880
	maxFPS100MbE = 148809
	maxFPS1GbE   = 1488096
	maxFPS10GbE  = 14880952
	// maximum bit per second constants for different Ethernet types
	maxBitPS10MbE  = 10000000
	maxBitPS100MbE = 100000000
	maxBitPS1GbE   = 1000000000
	maxBitPS10GbE  = 10000000000
	// makes it obvious during calculation what conversion is being
	// applied
	bitsPerByte = 8
)

func init() {
	deviceAttributes = map[int64]map[string]int64{
		10: map[string]int64{
			`fps`: maxFPS10MbE,
			`bps`: maxBitPS10MbE,
		},
		100: map[string]int64{
			`fps`: maxFPS100MbE,
			`bps`: maxBitPS100MbE,
		},
		1000: map[string]int64{
			`fps`: maxFPS1GbE,
			`bps`: maxBitPS1GbE,
		},
		10000: map[string]int64{
			`fps`: maxFPS10GbE,
			`bps`: maxBitPS10GbE,
		},
	}
}

// netIf implements the logic to compute derived network interface
// metrics
type netIf struct {
	assetID          int64
	curr             distribution
	next             distribution
	currTime         time.Time
	nextTime         time.Time
	speed            int64
	intf             string
	rxBPS            float64
	txBPS            float64
	rxPPS            float64
	txPPS            float64
	rxSize           int64
	txSize           int64
	rxUtilizationBPS float64
	txUtilizationBPS float64
	rxUtilizationPPS float64
	txUtilizationPPS float64
	utilization      float64 // net.utilization.percent:%dev
	lookup           *wall.Lookup
	ack              []*erebos.Transport
}

// Update adds m to the next distribution tracked by netIf
func (n *netIf) update(m *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {
	// set assetID on first use
	if n.assetID == 0 {
		n.assetID = m.AssetID
	}
	// set interface on first use
	if n.intf == `` {
		n.intf = m.Tags[0]
	}

	// check update has correct assetID
	if n.assetID != m.AssetID {
		// send back t so the offset gets committed
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}
	// check update has correct interface
	if n.intf != m.Tags[0] {
		// send back t so the offset gets committed
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

processing:
	// first metric for this distribution
	if n.nextTime.IsZero() {
		n.nextTime = m.TS
	}

	// out of order metric for old timestamp
	if n.nextTime.After(m.TS) {
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}

	// abandon current next and start new one
	if n.nextTime.Before(m.TS) {
		n.nextTime = time.Time{}
		n.next = distribution{}
		goto processing
	}

	if n.nextTime.Equal(m.TS) {
		switch m.Path {
		case `/sys/net/tx_bytes`:
			n.next.txBytes = m.Value().(int64)
			n.next.setTxBytes = true
		case `/sys/net/rx_bytes`:
			n.next.rxBytes = m.Value().(int64)
			n.next.setRxBytes = true
		case `/sys/net/tx_packets`:
			n.next.txPackets = m.Value().(int64)
			n.next.setTxPackets = true
		case `/sys/net/rx_packets`:
			n.next.rxPackets = m.Value().(int64)
			n.next.setRxPackets = true
		case `/sys/net/speed`:
			n.speed = m.Value().(int64)
		}
	}

	n.ack = append(n.ack, t)
	return n.calculate()
}

// calculate checks if the next counter has been fully assembled and
// then calculates the derived metrics, moves the counters forward and
// returns the derived metrics. If the next counter is not yet complete,
// it returns nil.
func (n *netIf) calculate() ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {

	if n.speed == 0 {
		return nil, nil, false, nil
	}

	if n.nextTime.IsZero() || !n.next.valid() {
		return nil, nil, false, nil
	}

	// do not walk backwards in time
	if n.currTime.After(n.nextTime) || n.currTime.Equal(n.nextTime) {
		return nil, nil, false, nil
	}

	// this is the initial update
	if n.currTime.IsZero() {
		n.nextToCurrent()
		return nil, nil, false, nil
	}

	deltaSeconds := n.nextTime.Sub(n.currTime).Seconds()
	// be paranoid
	if deltaSeconds <= 0 {
		n.nextToCurrent()
		return nil, nil, false, nil
	}

	// calculate moved bytes between sampling distributions
	rxBytes := n.next.rxBytes - n.curr.rxBytes
	txBytes := n.next.txBytes - n.curr.txBytes

	// calculate moved packets between sampling distributions
	rxPackets := n.next.rxPackets - n.curr.rxPackets
	txPackets := n.next.txPackets - n.curr.txPackets

	// check for counter wrap
	if rxBytes < 0 || txBytes < 0 || rxPackets < 0 || txPackets < 0 {
		n.nextToCurrent()
		return nil, nil, false, nil
	}

	// calculate average incoming packet size
	switch rxPackets {
	case 0:
		n.rxSize = 0
	default:
		n.rxSize = int64(float64(rxBytes) / float64(rxPackets))
	}

	// calculate average outgoing packet size
	switch txPackets {
	case 0:
		n.txSize = 0
	default:
		n.txSize = int64(float64(txBytes) / float64(txPackets))
	}

	// calculate bytes per second rates
	n.rxBPS = float64(rxBytes) / deltaSeconds
	n.txBPS = float64(txBytes) / deltaSeconds

	// calculate packets per second rates
	n.rxPPS = float64(rxPackets) / deltaSeconds
	n.txPPS = float64(txPackets) / deltaSeconds

	// only calculate utilizations if the device attributes are known
	if _, ok := deviceAttributes[n.speed]; ok && n.intf != `lo` {
		// calculate incoming bandwidth utilization
		rxUtilizationBPS := big.NewRat(0, 1).SetFrac64(
			int64(n.rxBPS)*bitsPerByte,
			deviceAttributes[n.speed][`bps`],
		)
		rxUtilizationBPS.Mul(rxUtilizationBPS, big.NewRat(100, 1))
		n.rxUtilizationBPS, _ = strconv.ParseFloat(rxUtilizationBPS.FloatString(2), 64)
		n.utilization = math.Max(n.utilization, n.rxUtilizationBPS)

		// calculate outgoing bandwidth utilization
		txUtilizationBPS := big.NewRat(0, 1).SetFrac64(
			int64(n.txBPS)*bitsPerByte,
			deviceAttributes[n.speed][`bps`],
		)
		txUtilizationBPS.Mul(txUtilizationBPS, big.NewRat(100, 1))
		n.txUtilizationBPS, _ = strconv.ParseFloat(txUtilizationBPS.FloatString(2), 64)
		n.utilization = math.Max(n.utilization, n.txUtilizationBPS)

		// calculate incoming packet rate utilization
		rxUtilizationPPS := big.NewRat(0, 1).SetFrac64(
			int64(n.rxPPS),
			deviceAttributes[n.speed][`fps`],
		)
		rxUtilizationPPS.Mul(rxUtilizationPPS, big.NewRat(100, 1))
		n.rxUtilizationPPS, _ = strconv.ParseFloat(rxUtilizationPPS.FloatString(2), 64)
		n.utilization = math.Max(n.utilization, n.rxUtilizationPPS)

		// calculate outgoing packet rate utilization
		txUtilizationPPS := big.NewRat(0, 1).SetFrac64(
			int64(n.txPPS),
			deviceAttributes[n.speed][`fps`],
		)
		txUtilizationPPS.Mul(txUtilizationPPS, big.NewRat(100, 1))
		n.txUtilizationPPS, _ = strconv.ParseFloat(txUtilizationPPS.FloatString(2), 64)
		n.utilization = math.Max(n.utilization, n.txUtilizationPPS)

		// round calculated utilization values
		n.rxUtilizationBPS = round(n.rxUtilizationBPS, .5, 2)
		n.txUtilizationBPS = round(n.txUtilizationBPS, .5, 2)
		n.rxUtilizationPPS = round(n.rxUtilizationPPS, .5, 2)
		n.txUtilizationPPS = round(n.txUtilizationPPS, .5, 2)
		n.utilization = round(n.utilization, .5, 2)
	}

	// round calculates values
	n.rxBPS = round(n.rxBPS, .5, 2)
	n.txBPS = round(n.txBPS, .5, 2)
	n.rxPPS = round(n.rxPPS, .5, 2)
	n.txPPS = round(n.txPPS, .5, 2)

	n.nextToCurrent()
	derived, err := n.emitMetric()
	if err != nil {
		return nil, nil, false, err
	}
	acks := n.ack
	n.ack = []*erebos.Transport{}
	return derived, acks, true, nil
}

// nextToCurrent advances the distributions within netIf by one step
func (n *netIf) nextToCurrent() {
	n.currTime = n.nextTime
	n.nextTime = time.Time{}

	n.curr = n.next
	n.next = distribution{}
}

// emitMetric returns the derived metrics for the current counter
func (n *netIf) emitMetric() ([]*legacy.MetricSplit, error) {
	nRxBPS := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.rx.bytes.per.second:%s", n.intf),
		TS:      n.currTime,
		Type:    `real`,
		Unit:    `Bps`,
		Val: legacy.MetricValue{
			FlpVal: n.rxBPS,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nRxBPS.LookupID(),
	); err == nil {
		// there are checks for this metric
		nRxBPS.Tags = tags
	} else if err != wall.ErrUnconfigured {
		// do not emit potentially incorrect metrics
		return []*legacy.MetricSplit{}, err
	}

	nTxBPS := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.tx.bytes.per.second:%s", n.intf),
		TS:      n.currTime,
		Type:    `real`,
		Unit:    `Bps`,
		Val: legacy.MetricValue{
			FlpVal: n.txBPS,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nTxBPS.LookupID(),
	); err == nil {
		nTxBPS.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	nRxPPS := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.rx.packets.per.second:%s", n.intf),
		TS:      n.currTime,
		Type:    `real`,
		Unit:    `Bps`,
		Val: legacy.MetricValue{
			FlpVal: n.rxPPS,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nRxPPS.LookupID(),
	); err == nil {
		nRxPPS.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	nTxPPS := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.tx.packets.per.second:%s", n.intf),
		TS:      n.currTime,
		Type:    `real`,
		Unit:    `Bps`,
		Val: legacy.MetricValue{
			FlpVal: n.txPPS,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nTxPPS.LookupID(),
	); err == nil {
		nTxPPS.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	nRxSize := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.rx.average.packet.size.bytes:%s", n.intf),
		TS:      n.currTime,
		Type:    `integer`,
		Unit:    `B`,
		Val: legacy.MetricValue{
			IntVal: n.rxSize,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nRxSize.LookupID(),
	); err == nil {
		nRxSize.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	nTxSize := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.tx.average.packet.size.bytes:%s", n.intf),
		TS:      n.currTime,
		Type:    `integer`,
		Unit:    `B`,
		Val: legacy.MetricValue{
			IntVal: n.txSize,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nTxSize.LookupID(),
	); err == nil {
		nTxSize.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	result := []*legacy.MetricSplit{nRxBPS, nTxBPS, nRxPPS, nTxPPS, nRxSize, nTxSize}

	// return result if utilizations have not been calculated
	if _, ok := deviceAttributes[n.speed]; !ok || n.intf == `lo` {
		return result, nil
	}

	nRxUtilBPS := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.rx.bandwidth.utilization.percent:%s", n.intf),
		TS:      n.currTime,
		Type:    `real`,
		Unit:    `%`,
		Val: legacy.MetricValue{
			FlpVal: n.rxUtilizationBPS,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nRxUtilBPS.LookupID(),
	); err == nil {
		nRxUtilBPS.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	nTxUtilBPS := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.tx.bandwidth.utilization.percent:%s", n.intf),
		TS:      n.currTime,
		Type:    `real`,
		Unit:    `%`,
		Val: legacy.MetricValue{
			FlpVal: n.txUtilizationBPS,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nTxUtilBPS.LookupID(),
	); err == nil {
		nTxUtilBPS.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	nRxUtilPPS := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.rx.packet.rate.utilization.percent:%s", n.intf),
		TS:      n.currTime,
		Type:    `real`,
		Unit:    `%`,
		Val: legacy.MetricValue{
			FlpVal: n.rxUtilizationPPS,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nRxUtilPPS.LookupID(),
	); err == nil {
		nRxUtilPPS.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	nTxUtilPPS := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.tx.packet.rate.utilization.percent:%s", n.intf),
		TS:      n.currTime,
		Type:    `real`,
		Unit:    `%`,
		Val: legacy.MetricValue{
			FlpVal: n.txUtilizationPPS,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nTxUtilPPS.LookupID(),
	); err == nil {
		nTxUtilPPS.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	nUtilization := &legacy.MetricSplit{
		AssetID: n.assetID,
		Path:    fmt.Sprintf("net.utilization.percent:%s", n.intf),
		TS:      n.currTime,
		Type:    `real`,
		Unit:    `%`,
		Val: legacy.MetricValue{
			FlpVal: n.utilization,
		},
	}
	if tags, err := n.lookup.GetConfigurationID(
		nUtilization.LookupID(),
	); err == nil {
		nUtilization.Tags = tags
	} else if err != wall.ErrUnconfigured {
		return []*legacy.MetricSplit{}, err
	}

	result = append(result, nRxUtilBPS, nTxUtilBPS, nRxUtilPPS, nTxUtilPPS, nUtilization)
	return result, nil
}

// distribution is used to track multiple network metrics from the same
// measurement cycle
type distribution struct {
	setRxBytes   bool
	setRxPackets bool
	setTxBytes   bool
	setTxPackets bool
	rxBytes      int64
	rxPackets    int64
	txBytes      int64
	txPackets    int64
}

// valid checks if a distribution has been fully populated
func (d *distribution) valid() bool {
	return d.setRxBytes && d.setRxPackets && d.setTxBytes && d.setTxPackets
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
