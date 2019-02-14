import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class Main {
    public static void main(String[] args) {
        Config config;
        try {
            config = new Config();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        KafkaCreator kafkaCreator = new KafkaCreator(config);

        KafkaConsumer<String, String> consumer = kafkaCreator.createConsumer();
        KafkaProducer<String, String> producer = kafkaCreator.createProducer();

        consumer.subscribe(Collections.singletonList(config.TOPIC));

        final boolean[] isRunning = { true };

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("info: shutting down");
            isRunning[0] = false;
        }));

        try {
            while (isRunning[0]) {
                ExecutorService executor = Executors.newFixedThreadPool(config.CONCURRENCY);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                System.out.println("debug: consumed " + records.count() + " messages");

                Iterable<ConsumerRecord<String, String>> consumerRecords = config.SHOULD_DEDUP_BY_KEY
                        ? StreamSupport.stream(records.spliterator(), false)
                                .collect(Collectors.groupingBy(ConsumerRecord::key)).entrySet().stream()
                                .map(x -> x.getValue().get(0)).collect(Collectors.toList())
                        : records;

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    executor.submit(new ConsumerRecordRunnable(record, config, producer));
                }

                executor.shutdown();

                while (true) {
                    if (executor.isTerminated())
                        break;
                }
                try {
                    consumer.commitSync();
                } catch (CommitFailedException ignored) {
                    System.out.println("info: commit failed");
                }
            }
        } catch (Exception e) {
            System.out.println("error: unexpected error occured: " + e.getMessage());

        } finally {
            consumer.unsubscribe();
            consumer.close();
        }
    }
}