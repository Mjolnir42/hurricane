/*-
 * Copyright © 2018, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package netif // import "github.com/mjolnir42/hurricane/internal/netif"

import (
	"github.com/mjolnir42/erebos"
	wall "github.com/mjolnir42/eye/lib/eye.wall"
	"github.com/mjolnir42/hurricane/internal/intf"
	"github.com/mjolnir42/legacy"
)

// Implementation of the intf.Deriver interface

// NewDeriver returns a new Deriver
func NewDeriver(conf *erebos.Config) *Deriver {
	d := &Deriver{}
	d.data = make(map[int64]map[string]*netIf)
	d.lookup = wall.NewLookup(conf, `hurricane`)
	return d
}

// Deriver holds the metric distributions used to calculate derived
// network interface metrics
type Deriver struct {
	data   map[int64]map[string]*netIf
	lookup *wall.Lookup
}

// Start activates the embedded cache lookup in d
func (d *Deriver) Start() error {
	return d.lookup.Start()
}

// Close shuts down the embedded cache lookup in d
func (d *Deriver) Close() {
	d.lookup.Close()
}

// Register adds the metrics d wants to consume into m
func (d *Deriver) Register(m map[string]intf.Deriver) {
	for _, s := range []string{
		`/sys/net/tx_bytes`,
		`/sys/net/tx_packets`,
		`/sys/net/rx_bytes`,
		`/sys/net/rx_packets`,
		`/sys/net/speed`,
	} {
		m[s] = d
	}
}

// Update provides d with a new metric
func (d *Deriver) Update(m *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {
	if len(m.Tags) == 0 {
		// tag 0 should be the interface:
		// [ 97612, "/sys/net/tx_bytes", "2017-05-25T11:04:45Z", "integer", "", 10394624195, [ "eth0" ], null ]
		// [ 97612, "/sys/net/tx_packets", "2017-05-25T11:05:00Z", "integer", "", 79520209, [ "eth0" ], null ]
		// [ 97612, "/sys/net/rx_bytes", "2017-05-25T11:05:00Z", "integer", "", 53265442364, [ "eth0" ], null ]
		// [ 97612, "/sys/net/rx_packets", "2017-05-25T11:05:00Z", "integer", "", 107083836, [ "eth0" ], null ]
		// [ 97612, "/sys/net/speed", "2017-05-25T11:04:45Z", "integer", "", 1000, [ "eth0" ], null ]
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}
	intf := m.Tags[0]

	if _, ok := d.data[m.AssetID]; !ok {
		d.data[m.AssetID] = make(map[string]*netIf)
	}

	if _, ok := d.data[m.AssetID][intf]; !ok {
		d.data[m.AssetID][intf] = &netIf{
			lookup: d.lookup,
		}
	}

	return d.data[m.AssetID][intf].update(m, t)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
