/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package hurricane // import "github.com/mjolnir42/hurricane/internal/hurricane"

import (
	"github.com/Sirupsen/logrus"
	metrics "github.com/rcrowley/go-metrics"
)

// run is the event loop for Hurricane
func (h *Hurricane) run() {
	in := metrics.GetOrRegisterMeter(
		`/input/messages.per.second`,
		*h.Metrics,
	)
	out := metrics.GetOrRegisterMeter(
		`/output/messages.per.second`,
		*h.Metrics,
	)

	// required during shutdown
	inputEmpty := false
	errorEmpty := false
	successEmpty := false
	producerClosed := false

runloop:
	for {
		select {
		case <-h.Shutdown:
			// received shutdown, drain input channel which will be
			// closed by main
			goto drainloop
		case err := <-h.producer.Errors():
			h.Death <- err
			<-h.Shutdown
			break runloop
		case msg := <-h.producer.Successes():
			trackingID := msg.Metadata.(string)
			h.updateOffset(trackingID)
			out.Mark(1)
		case msg := <-h.Input:
			if msg == nil {
				// this can happen if we read the closed Input channel
				// before the closed Shutdown channel
				continue runloop
			}
			h.process(msg)
			in.Mark(1)
		}
	}
	// shutdown due to producer error
	h.producer.Close()
	return

drainloop:
	for {
		select {
		case msg := <-h.Input:
			if msg == nil {
				// channel is closed
				inputEmpty = true

				if !producerClosed {
					h.producer.Close()
					producerClosed = true
				}

				// channels are closed
				if inputEmpty && errorEmpty && successEmpty {
					break drainloop
				}
				continue drainloop
			}
			h.process(msg)
		case e := <-h.producer.Errors():
			if e == nil {
				errorEmpty = true

				// channels are closed
				if inputEmpty && errorEmpty && successEmpty {
					break drainloop
				}
				continue drainloop
			}
			logrus.Errorln(e)
		case msg := <-h.producer.Successes():
			if msg == nil {
				successEmpty = true

				// channels are closed
				if inputEmpty && errorEmpty && successEmpty {
					break drainloop
				}
				continue drainloop
			}
			trackingID := msg.Metadata.(string)
			h.updateOffset(trackingID)
			out.Mark(1)
		}
	}
	h.delay.Wait()
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
