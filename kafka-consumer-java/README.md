# kafka-consumer-java

## Generate new Proto

```
./protoc --proto_path=$PWD/soluto-kafka-grpc-target --java_out=$PWD/kafka-consumer-java/src/main/java $PWD/soluto-kafka-grpc-target/message.proto
```
