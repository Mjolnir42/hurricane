/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package ctx // import "github.com/solnx/hurricane/internal/ctx"

import (
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	wall "github.com/solnx/eye/lib/eye.wall"
	"github.com/solnx/hurricane/internal/intf"
)

// Implementation of the intf.Deriver interface

// NewDeriver ...
func NewDeriver(conf *erebos.Config) *Deriver {
	d := &Deriver{}
	d.Data = make(map[int64]*CTX)
	d.lookup = wall.NewLookup(conf, `hurricane`)
	return d
}

// Deriver ...
type Deriver struct {
	Data   map[int64]*CTX
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
	m[`/sys/cpu/ctx`] = d
}

// Update ...
func (d *Deriver) Update(m *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool, error) {
	if _, ok := d.Data[m.AssetID]; !ok {
		d.Data[m.AssetID] = &CTX{
			lookup: d.lookup,
		}
	}

	return d.Data[m.AssetID].update(m, t)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
