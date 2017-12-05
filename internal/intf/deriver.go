/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package intf provides interface definitions without causing an import
// loop
package intf // import "github.com/mjolnir42/hurricane/internal/intf"

import (
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
)

// Deriver is the interface for packages that calculate derived metrics
type Deriver interface {
	// Using Register the deriver sets itself as handler for its metrics
	Register(m map[string]Deriver)
	// Update provides the Deriver with a new input metric. If there are
	// derived metrics or message offsets to handle it will also return
	// true.
	Update(m *legacy.MetricSplit, t *erebos.Transport) ([]*legacy.MetricSplit, []*erebos.Transport, bool, error)
	// Connect the embedded redis client
	Start() error
	// Close the embedded redis client
	Close()
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
