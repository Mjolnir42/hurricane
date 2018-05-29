/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package disk // import "github.com/mjolnir42/hurricane/internal/disk"

import (
	"github.com/mjolnir42/erebos"
	wall "github.com/mjolnir42/eye/lib/eye.wall"
	"github.com/mjolnir42/hurricane/internal/intf"
	"github.com/mjolnir42/legacy"
)

// Implementation of the intf.Deriver interface

// NewDeriver ...
func NewDeriver(conf *erebos.Config) *Deriver {
	d := &Deriver{}
	d.data = make(map[int64]map[string]*dsk)
	d.lookup = wall.NewLookup(conf, `hurricane`)
	return d
}

// Deriver ...
type Deriver struct {
	data   map[int64]map[string]*dsk
	lookup *wall.Lookup
}

// Start ...
func (d *Deriver) Start() error {
	return d.lookup.Start()
}

// Close ...
func (d *Deriver) Close() {
	d.lookup.Close()
}

// Register ...
func (d *Deriver) Register(m map[string]intf.Deriver) {
	for _, s := range []string{
		`/sys/disk/blk_total`,
		`/sys/disk/blk_used`,
		`/sys/disk/blk_read`,
		`/sys/disk/blk_wrtn`,
	} {
		m[s] = d
	}
}

// Update ...
func (d *Deriver) Update(m *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {
	if len(m.Tags) == 0 {
		// valid disk metrics require their mountpoint as tag 0
		return []*legacy.MetricSplit{}, []*erebos.Transport{t}, true, nil
	}
	mpt := m.Tags[0]

	if _, ok := d.data[m.AssetID]; !ok {
		d.data[m.AssetID] = make(map[string]*dsk)
	}

	if _, ok := d.data[m.AssetID][mpt]; !ok {
		d.data[m.AssetID][mpt] = &dsk{
			lookup: d.lookup,
		}
	}

	return d.data[m.AssetID][mpt].update(m, t)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
