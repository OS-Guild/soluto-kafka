import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {
    public static void main(String[] args) {
        Config config;
        try {
            config = new Config();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        KafkaConsumer<String, String> consumer = ConsumerCreator.create(config);
        consumer.subscribe(Collections.singletonList(config.TOPIC));

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    consumer.unsubscribe();
                    consumer.close();
                }));

        while (true) {
            ExecutorService executor = Executors.newFixedThreadPool(config.CONCURRENCY);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            System.out.println(records.count());

            Iterable<ConsumerRecord<String, String>> consumerRecords = config.SHOULD_DEDUP_BY_KEY
                    ? StreamSupport.stream(records.spliterator(), false)
                    .collect(Collectors.groupingBy(ConsumerRecord::key))
                    .entrySet()
                    .stream()
                    .map(x -> x.getValue().get(0))
                    .collect(Collectors.toList())
                    : records;

            for (ConsumerRecord<String, String> record : consumerRecords) {
                executor.submit(new ConsumerRecordRunnable(record, config));
            }

            executor.shutdown();

            while (true) {
                if (executor.isTerminated())
                    break;
            }

             consumer.commitSync();
        }
    }
}