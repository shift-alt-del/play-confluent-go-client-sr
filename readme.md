# Play with Confluent-go-client with SR support (>=1.9.1)

This repo provides 4 demos producer, consumer with avro, protobuf message format.

## Setup

1. It needs environment varialbles as below:
   ```
   BOOTSTRAP_SERVERS="pkc-xxx.xxxxxx.confluent.cloud:9092"
   SASL_USERNAME="XXXXXXX"
   SASL_PASSWORD="XXXXXXX"
   SR_URL="https://psrc-xxxxxx.confluent.cloud"
   SR_USERNAME="ZZZZZZZ"
   SR_PASSWORD="ZZZZZZZ"
   ```
2. Create topics:
   ```
   confluent kafka topic create public.cc.sr.avro.demo
   confluent kafka topic create public.cc.sr.pb.demo
   ```

## References:
- https://github.com/confluentinc/confluent-kafka-go
- https://github.com/confluentinc/confluent-kafka-go/tree/master/examples