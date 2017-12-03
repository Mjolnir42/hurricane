/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package ctx // import "github.com/mjolnir42/hurricane/internal/ctx"

import (
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/hurricane/internal/intf"
	"github.com/mjolnir42/legacy"
)

// Implementation of the intf.Deriver interface

// NewDeriver ...
func NewDeriver() *Deriver {
	d := &Deriver{}
	d.Data = make(map[int64]*CTX)
	return d
}

// Deriver ...
type Deriver struct {
	Data map[int64]*CTX
}

// Register ...
func (d *Deriver) Register(m map[string]intf.Deriver) {
	m[`/sys/cpu/ctx`] = d
}

// Update ...
func (d *Deriver) Update(m *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool) {
	if _, ok := d.Data[m.AssetID]; !ok {
		d.Data[m.AssetID] = &CTX{}
	}

	return d.Data[m.AssetID].update(m, t)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
