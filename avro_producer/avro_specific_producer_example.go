// Example function-based Apache Kafka producer
package main

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

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/avro"
)

func main() {

	topic := "public.cc.sr.avro.demo"
	bootstrapServers := os.Getenv("BOOTSTRAP_SERVERS")
	saslUsername := os.Getenv("SASL_USERNAME")
	saslPassword := os.Getenv("SASL_PASSWORD")
	srURL := os.Getenv("SR_URL")
	srUsername := os.Getenv("SR_USERNAME")
	srPassword := os.Getenv("SR_PASSWORD")

	// config producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     bootstrapServers,
		"security.protocol":                     "SASL_SSL",
		"sasl.mechanism":                        "PLAIN",
		"sasl.username":                         saslUsername,
		"sasl.password":                         saslPassword,
		"ssl.endpoint.identification.algorithm": "https",
		"enable.ssl.certificate.verification":   "false",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Producer %v\n", p)

	// config SR client
	client, err := schemaregistry.NewClient(&schemaregistry.Config{
		SchemaRegistryURL:          srURL,
		BasicAuthCredentialsSource: "USER_INFO",
		BasicAuthUserInfo:          srUsername + ":" + srPassword,
	})

	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	// config serializer
	ser, err := avro.NewSpecificSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())

	if err != nil {
		fmt.Printf("Failed to create serializer: %s\n", err)
		os.Exit(1)
	}

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	value := User{
		Name:            "First user",
		Favorite_number: 42,
		Favorite_color:  "blue",
	}
	payload, err := ser.Serialize(topic, &value)
	if err != nil {
		fmt.Printf("Failed to serialize payload: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Serialized %v\n", payload)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)
	if err != nil {
		fmt.Printf("Produce failed: %v\n", err)
		os.Exit(1)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
}
