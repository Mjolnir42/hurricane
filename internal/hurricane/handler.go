/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package hurricane // import "github.com/mjolnir42/hurricane/internal/hurricane"

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	wall "github.com/mjolnir42/eye/lib/eye.wall"
	"github.com/mjolnir42/hurricane/internal/cpu"
	"github.com/mjolnir42/hurricane/internal/ctx"
	"github.com/mjolnir42/hurricane/internal/disk"
	"github.com/mjolnir42/hurricane/internal/mem"
	kazoo "github.com/wvanbergen/kazoo-go"
)

// Implementation of the erebos.Handler interface

// Start sets up the Hurricane application
func (h *Hurricane) Start() {
	if len(Handlers) == 0 {
		h.Death <- fmt.Errorf(`Incorrectly set handlers`)
		<-h.Shutdown
		return
	}

	kz, err := kazoo.NewKazooFromConnectionString(
		h.Config.Zookeeper.Connect, nil)
	if err != nil {
		h.Death <- err
		<-h.Shutdown
		return
	}
	brokers, err := kz.BrokerList()
	if err != nil {
		kz.Close()
		h.Death <- err
		<-h.Shutdown
		return
	}
	kz.Close()

	host, err := os.Hostname()
	if err != nil {
		h.Death <- err
		<-h.Shutdown
		return
	}

	config := sarama.NewConfig()
	// set transport keepalive
	switch h.Config.Kafka.Keepalive {
	case 0:
		config.Net.KeepAlive = 3 * time.Second
	default:
		config.Net.KeepAlive = time.Duration(
			h.Config.Kafka.Keepalive,
		) * time.Millisecond
	}
	// set our required persistence confidence for producing
	switch h.Config.Kafka.ProducerResponseStrategy {
	case `NoResponse`:
		config.Producer.RequiredAcks = sarama.NoResponse
	case `WaitForLocal`:
		config.Producer.RequiredAcks = sarama.WaitForLocal
	case `WaitForAll`:
		config.Producer.RequiredAcks = sarama.WaitForAll
	default:
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}

	// set return parameters
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	// set how often to retry producing
	switch h.Config.Kafka.ProducerRetry {
	case 0:
		config.Producer.Retry.Max = 3
	default:
		config.Producer.Retry.Max = h.Config.Kafka.ProducerRetry
	}
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.ClientID = fmt.Sprintf("hurricane.%s", host)

	h.trackID = make(map[string]int)
	h.trackACK = make(map[string][]*erebos.Transport)

	h.producer, err = sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		h.Death <- err
		<-h.Shutdown
		return
	}
	h.dispatch = h.producer.Input()
	h.delay = delay.New()

	h.lookup = wall.NewLookup(h.Config, `hurricane`)
	defer h.lookup.Close()

	if h.Config.Hurricane.DeriveCTX {
		ctxDeriver := ctx.NewDeriver(h.Config)
		if err := ctxDeriver.Start(); err != nil {
			h.Death <- err
			<-h.Shutdown
			return
		}
		ctxDeriver.Register(h.deriver)
		defer ctxDeriver.Close()
	}

	if h.Config.Hurricane.DeriveCPU {
		cpuDeriver := cpu.NewDeriver(h.Config)
		if err := cpuDeriver.Start(); err != nil {
			h.Death <- err
			<-h.Shutdown
			return
		}
		cpuDeriver.Register(h.deriver)
		defer cpuDeriver.Close()
	}

	if h.Config.Hurricane.DeriveMEM {
		memDeriver := mem.NewDeriver(h.Config)
		if err := memDeriver.Start(); err != nil {
			h.Death <- err
			<-h.Shutdown
			return
		}
		memDeriver.Register(h.deriver)
		defer memDeriver.Close()
	}

	if h.Config.Hurricane.DeriveDISK {
		dskDeriver := disk.NewDeriver(h.Config)
		if err := dskDeriver.Start(); err != nil {
			h.Death <- err
			<-h.Shutdown
			return
		}
		dskDeriver.Register(h.deriver)
		defer dskDeriver.Close()
	}

	h.run()
}

// InputChannel returns the data input channel
func (h *Hurricane) InputChannel() chan *erebos.Transport {
	return h.Input
}

// ShutdownChannel returns the shutdown signal channel
func (h *Hurricane) ShutdownChannel() chan struct{} {
	return h.Shutdown
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
