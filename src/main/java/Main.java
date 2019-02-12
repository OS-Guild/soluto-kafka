import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {
    public static void main(String[] args) {
        int threadCount = 10;

        Properties props = new Properties();
        props.put("bootstrap.servers", "soluto-kafka-1-operations-6e99.aivencloud.com:20273");
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "client.truststore.jks");
        props.put("ssl.truststore.password", "aadd77a7-ec6c-427d-8d72-2aa4046fee62");
        props.put("ssl.keystore.type", "PKCS12");
        props.put("ssl.keystore.location", "client.keystore.p12");
        props.put("ssl.keystore.password", "aadd77a7-ec6c-427d-8d72-2aa4046fee62");
        props.put("ssl.key.password", "aadd77a7-ec6c-427d-8d72-2aa4046fee62");
        props.put("group.id", "demo-group-6");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("expert-queue"));

        while (true) {
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            List<ConsumerRecord<String, String>> consumerRecords = StreamSupport.stream(records.spliterator(), false)
                    .collect(Collectors.groupingBy(ConsumerRecord::key))
                    .entrySet()
                    .stream()
                    .map(x -> x.getValue().get(0))
                    .collect(Collectors.toList());

            System.out.println(consumerRecords.size() + "/" + records.count());

            for (ConsumerRecord<String, String> record : consumerRecords) {
                executor.submit(new ConsumerThreadHandler(record));
            }

            executor.shutdown();
            while (!executor.isTerminated()) {}

            consumer.commitSync();
        }
    }
}