/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package hurricane calculates derived metrics
package hurricane // import "github.com/mjolnir42/hurricane/internal/hurricane"

import (
	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/hurricane/internal/intf"
	metrics "github.com/rcrowley/go-metrics"
)

// Handlers is the registry of running application handlers
var Handlers map[int]erebos.Handler

// init function sets up package variables
func init() {
	// Handlers tracks all Hurricane instances and is used by
	// Dispatch() to find the correct instance to route the message to
	Handlers = make(map[int]erebos.Handler)
}

// Hurricane calculates and produces derived metrics
type Hurricane struct {
	Num      int
	Input    chan *erebos.Transport
	Shutdown chan struct{}
	Death    chan error
	Config   *erebos.Config
	Metrics  *metrics.Registry
	// unexported
	delay    *delay.Delay
	deriver  map[string]intf.Deriver
	trackID  map[string]int
	trackACK map[string][]*erebos.Transport
	dispatch chan<- *sarama.ProducerMessage
	producer sarama.AsyncProducer
}

// updateOffset updates the consumer offsets in Kafka once all
// outstanding messages for trackingID have been processed
func (h *Hurricane) updateOffset(trackingID string) {
	if _, ok := h.trackID[trackingID]; !ok {
		logrus.Warnf("Unknown trackingID: %s", trackingID)
		return
	}
	// decrement outstanding successes for trackingID
	h.trackID[trackingID]--
	// check if trackingID has been fully processed
	if h.trackID[trackingID] == 0 {
		// commit processed offsets to Zookeeper
		acks := h.trackACK[trackingID]
		for i := range acks {
			h.delay.Use()
			go func(idx int) {
				h.commit(acks[idx])
				h.delay.Done()
			}(i)
		}
		// cleanup offset tracking
		delete(h.trackID, trackingID)
		delete(h.trackACK, trackingID)
	}
}

// commit marks a message as fully processed
func (h *Hurricane) commit(msg *erebos.Transport) {
	msg.Commit <- &erebos.Commit{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
