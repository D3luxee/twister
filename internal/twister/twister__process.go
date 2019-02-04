/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package twister // import "github.com/solnx/twister/internal/twister"

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/d3luxee/schema"
	"github.com/mjolnir42/erebos"
	uuid "github.com/satori/go.uuid"
	wall "github.com/solnx/eye/lib/eye.wall"
	"github.com/solnx/legacy"
)

// process is the handler for converting a MetricBatch
// and producing the result. Invalid data is marked as processed
// and skipped.
func (t *Twister) process(msg *erebos.Transport) {
	if msg == nil || msg.Value == nil {
		logrus.Warnf("Ignoring empty message from: %d", msg.HostID)
		if msg != nil {
			t.delay.Use()
			go func() {
				t.commit(msg)
				t.delay.Done()
			}()
		}
		return
	}

	// handle heartbeat messages
	if erebos.IsHeartbeat(msg) {
		t.delay.Use()
		go func() {
			t.lookup.Heartbeat(func() string {
				switch t.Config.Misc.InstanceName {
				case ``:
					return `twister`
				default:
					return fmt.Sprintf("twister/%s",
						t.Config.Misc.InstanceName)
				}
			}(), t.Num, msg.Value)
			t.delay.Done()
		}()
		return
	}

	batch := legacy.MetricBatch{}
	if err := json.Unmarshal(msg.Value, &batch); err != nil {
		logrus.Warnf("Ignoring invalid data: %s", err.Error())
		t.delay.Use()
		go func() {
			t.commit(msg)
			t.delay.Done()
		}()
		return
	}

	// panic on entropy error
	trackingID := uuid.Must(uuid.NewV4()).String()
	var produced int

	msgs := batch.Split()
	for i := range msgs {

		if t.lookKeys[msgs[i].Path] {
			if tags, err := t.lookup.GetConfigurationID(
				msgs[i].LookupID(),
			); err == nil {
				msgs[i].Tags = append(msgs[i].Tags, tags...)
			} else if err != wall.ErrUnconfigured {
				t.Death <- err
				<-t.Shutdown
				return
			}
		}
		var data []byte
		var m2 schema.MetricData
		var err error
		if msgs[i].IsMetrics20() && msgs[i].Type != "string" {
			m2, err = msgs[i].ConvertToMetrics20()
			if err != nil {
				logrus.Warnf("Ignoring invalid data: %s", err.Error())
				logrus.Debugln(`Ignored data:`, msgs[i])
				continue
			}
			data, err = m2.MarshalMsg(data[:])
			if err != nil {
				logrus.Warnf("Ignoring invalid data: %s", err.Error())
				logrus.Debugln(`Ignored data:`, msgs[i])
				continue
			}
		} else {
			data, err = json.Marshal(&msgs[i])
			if err != nil {
				logrus.Warnf("Ignoring invalid data: %s", err.Error())
				logrus.Debugln(`Ignored data:`, msgs[i])
				continue
			}
		}

		t.delay.Use()
		go func(idx int, data []byte) {
			if msgs[idx].IsMetrics20() {
				t.dispatch <- &sarama.ProducerMessage{
					Topic:    t.Config.Twister.Metrics20TopicPrefix + t.Config.Kafka.ProducerTopic,
					Key:      sarama.ByteEncoder(m2.KeyBySeries(nil)),
					Value:    sarama.ByteEncoder(data),
					Metadata: trackingID,
				}
				t.delay.Done()
			} else {
				t.dispatch <- &sarama.ProducerMessage{
					Topic: t.Config.Kafka.ProducerTopic,
					Key: sarama.StringEncoder(
						strconv.Itoa(int(msgs[idx].AssetID)),
					),
					Value:    sarama.ByteEncoder(data),
					Metadata: trackingID,
				}
				t.delay.Done()
			}
		}(i, data)
		produced++
	}

	// if no metrics were produced, commit offset immediately
	if produced == 0 {
		go func() {
			t.commit(msg)
			t.delay.Done()
		}()
		return
	}
	// store offsets until AsyncProducer returns success
	t.trackID[trackingID] = produced
	t.trackACK[trackingID] = []*erebos.Transport{msg}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
