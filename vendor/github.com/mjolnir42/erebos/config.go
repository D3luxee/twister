/*-
 * Copyright © 2016-2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package erebos

import (
	"bytes"
	"encoding/json"
	"io/ioutil"

	ucl "github.com/nahanni/go-ucl"
)

// Config holds the runtime configuration which is expected to be
// read from a UCL formatted file
type Config struct {
	// Zookeeper is the namespace with options for Apache Zookeeper
	Zookeeper struct {
		// How often to publish offset updates to Zookeeper
		CommitInterval int `json:"commit.ms,string"`
		// Conncect string for the Zookeeper-Ensemble to use
		Connect string `json:"connect.string"`
		// If true, the Zookeeper stored offset will be ignored and
		// the newest message consumed
		ResetOffset bool `json:"reset.offset.on.startup,string"`
	} `json:"zookeeper"`
	// Kafka is the namespace with options for Apache Kafka
	Kafka struct {
		// Name of the consumergroup to join
		ConsumerGroup string `json:"kafka.consumer.group.name"`
		// Which topics to consume from
		ConsumerTopics string `json:"kafka.consumer.topics"`
		// Which topic to produce to
		ProducerTopic string `json:"kafka.producer.topic"`
	} `json:"kafka"`
	// Twister is the namespace with configuration options relating to
	// the splitting of metric batches
	Twister struct {
		HandlerQueueLength int `json:"handler.queue.length,string"`
	} `json:"twister"`
	// Mistral is the namespace with configuration options relating to
	// accepting incoming messages via HTTP API
	Mistral struct {
		HandlerQueueLength int    `json:"handler.queue.length,string"`
		ListenAddress      string `json:"listen.address"`
		ListenPort         string `json:"listen.port"`
		EndpointPath       string `json:"api.endpoint.path"`
	} `json:"mistral"`
}

// FromFile sets Config c based on the file contents
func (c *Config) FromFile(fname string) error {
	var (
		file, uclJSON []byte
		err           error
		fileBytes     *bytes.Buffer
		parser        *ucl.Parser
		uclData       map[string]interface{}
	)
	if file, err = ioutil.ReadFile(fname); err != nil {
		return err
	}

	fileBytes = bytes.NewBuffer(file)
	parser = ucl.NewParser(fileBytes)
	if uclData, err = parser.Ucl(); err != nil {
		return err
	}

	if uclJSON, err = json.Marshal(uclData); err != nil {
		return err
	}
	return json.Unmarshal(uclJSON, &c)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
