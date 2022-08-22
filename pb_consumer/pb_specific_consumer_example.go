/**
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Example function-based high-level Apache Kafka consumer
package main

// consumer_example implements a consumer using the non-channel Poll() API
// to retrieve messages and events.

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"github.com/shift-alt-del/play-confluent-go-client-sr/pb_consumer/resources/generated"
)

func main() {

	topic := "public.cc.sr.pb.demo"
	bootstrapServers := os.Getenv("BOOTSTRAP_SERVERS")
	saslUsername := os.Getenv("SASL_USERNAME")
	saslPassword := os.Getenv("SASL_PASSWORD")
	srURL := os.Getenv("SR_URL")
	srUsername := os.Getenv("SR_USERNAME")
	srPassword := os.Getenv("SR_PASSWORD")

	groupId := "mygroup"

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                     bootstrapServers,
		"security.protocol":                     "SASL_SSL",
		"sasl.mechanism":                        "PLAIN",
		"sasl.username":                         saslUsername,
		"sasl.password":                         saslPassword,
		"ssl.endpoint.identification.algorithm": "https",
		"enable.ssl.certificate.verification":   "false",
		"group.id":                              groupId,
		"session.timeout.ms":                    6000,
		"auto.offset.reset":                     "earliest",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	client, err := schemaregistry.NewClient(&schemaregistry.Config{
		SchemaRegistryURL:          srURL,
		BasicAuthCredentialsSource: "USER_INFO",
		BasicAuthUserInfo:          srUsername + ":" + srPassword,
	})

	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	deser, err := protobuf.NewDeserializer(client, serde.ValueSerde, protobuf.NewDeserializerConfig())

	if err != nil {
		fmt.Printf("Failed to create deserializer: %s\n", err)
		os.Exit(1)
	}

	err = c.SubscribeTopics([]string{topic}, nil)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				value := generated.User{}
				err := deser.DeserializeInto(*e.TopicPartition.Topic, e.Value, &value)
				if err != nil {
					fmt.Printf("Failed to deserialize payload: %s\n", err)
				} else {
					fmt.Printf("%% Message on %s:\n%+v\n", e.TopicPartition, value)
				}
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
