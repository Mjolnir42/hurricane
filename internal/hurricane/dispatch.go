/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package hurricane // import "github.com/solnx/hurricane/internal/hurricane"

import (
	"runtime"

	"github.com/mjolnir42/erebos"
	"github.com/solnx/legacy"
)

// Implementation of the erebos.Dispatcher interface

// Dispatch routes msg to the correct Handler instance
func Dispatch(msg erebos.Transport) error {
	// send all messages from the same host to the same handler
	hostID, err := legacy.PeekHostID(msg.Value)
	if err != nil {
		return err
	}
	msg.HostID = hostID

	Handlers[hostID%runtime.NumCPU()].InputChannel() <- &msg
	return nil
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
