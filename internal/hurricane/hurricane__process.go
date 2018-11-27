/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package hurricane // import "github.com/solnx/hurricane/internal/hurricane"

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/mjolnir42/erebos"
	uuid "github.com/satori/go.uuid"
	"github.com/solnx/legacy"
)

// process is the handler for deriving metrics and producing the
// result. Invalid data is marked as processed and skipped.
func (h *Hurricane) process(msg *erebos.Transport) {
	if msg == nil || msg.Value == nil {
		logrus.Warnf("Ignoring empty message from: %d", msg.HostID)
		if msg != nil {
			h.delay.Use()
			go func() {
				h.commit(msg)
				h.delay.Done()
			}()
		}
		return
	}

	// handle heartbeat messages
	if erebos.IsHeartbeat(msg) {
		h.delay.Use()
		go func() {
			h.lookup.Heartbeat(func() string {
				switch h.Config.Misc.InstanceName {
				case ``:
					return `hurricane`
				default:
					return fmt.Sprintf("hurricane/%s",
						h.Config.Misc.InstanceName)
				}
			}(), h.Num, msg.Value)
			h.delay.Done()
		}()
		return
	}

	m := &legacy.MetricSplit{}
	if err := json.Unmarshal(msg.Value, m); err != nil {
		logrus.Warnf("Ignoring invalid data: %s", err.Error())
		h.delay.Use()
		go func() {
			h.commit(msg)
			h.delay.Done()
		}()
		return
	}

	if _, ok := h.deriver[m.Path]; !ok {
		// no Deriver interested in this metric
		h.delay.Use()
		go func() {
			h.commit(msg)
			h.delay.Done()
		}()
		return
	}

	if derived, acks, ok, err := h.deriver[m.Path].Update(m, msg); ok {
		trackingID := uuid.Must(uuid.NewV4()).String()
		var produced int

		for i := range derived {
			data, e := json.Marshal(&derived[i])
			if e != nil {
				logrus.Warnf("Ignoring invalid data: %s",
					e.Error())
				logrus.Debugln(`Ignored data:`, derived[i])
				continue
			}

			h.delay.Use()
			go func(idx int, data []byte) {
				h.dispatch <- &sarama.ProducerMessage{
					Topic: h.Config.Kafka.ProducerTopic,
					Key: sarama.StringEncoder(
						strconv.Itoa(int(derived[idx].AssetID)),
					),
					Value:    sarama.ByteEncoder(data),
					Metadata: trackingID,
				}
				h.delay.Done()
			}(i, data)
			produced++
		}

		// if no metrics were produced, commit ACKs immediately
		if produced == 0 {
			for i := range acks {
				h.delay.Use()
				go func(idx int) {
					h.commit(acks[idx])
					h.delay.Done()
				}(i)
			}
			return
		}
		// store ACKs until AsyncProducer returns success
		h.trackID[trackingID] = produced
		h.trackACK[trackingID] = acks
	} else if err != nil {
		// error from the eyewall lookup
		h.Death <- err
		<-h.Shutdown
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
