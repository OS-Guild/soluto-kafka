import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.mashape.unirest.http.Unirest;

import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class Main {
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        WriteLog writeLog = new WriteLog();
        WriteMetric writeMetric = new WriteMetric(config);
        Unirest.setHttpClient(HttpClients.createDefault());
        Unirest.setDefaultHeader("Connection", "keep-alive");
        
        KafkaCreator kafkaCreator = new KafkaCreator(config);
        KafkaConsumer<String, String> consumer = kafkaCreator.createConsumer();
        KafkaProducer<String, String> producer = kafkaCreator.createProducer();

        consumer.subscribe(Collections.singletonList(config.TOPIC));
        writeLog.serviceStarted(config);

        final boolean[] isRunning = { true };

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning[0] = false;
        }));

        try {
            while (isRunning[0]) {
                ExecutorService executor = Executors.newFixedThreadPool(config.CONCURRENCY);
                ConsumerRecords<String, String> consumed = consumer.poll(Duration.ofMillis(config.CONSUMER_POLL_TIMEOUT));
                

                writeLog.consumed(consumed);
                writeMetric.consumed(consumed);
                Iterable<ConsumerRecord<String, String>> records = preprocess(config, consumed);
                writeMetric.consumedDedup(records);
                
                for (ConsumerRecord<String, String> record : records) {
                    writeMetric.messageLatency(record);
                    executor.submit(new ConsumerRecordRunnable(config, writeMetric, writeLog, producer, record));
                }

                executor.shutdown();

                while (true) {
                    if (executor.isTerminated())
                        break;
                }
                try {
                    consumer.commitSync();
                } catch (CommitFailedException ignored) {
                    writeLog.commitFailed();
                }
            }
        } catch (Exception e) {
            writeLog.unexpectedErorr(e);
        } finally {
            consumer.unsubscribe();
            consumer.close();
            writeLog.serviceShutdown(config);
        }
    }

    private static Iterable<ConsumerRecord<String, String>> preprocess(Config config, ConsumerRecords<String, String> records) {
        return config.SHOULD_DEDUP_BY_KEY
            ? StreamSupport.stream(records.spliterator(), false)
                    .collect(Collectors.groupingBy(ConsumerRecord::key)).entrySet().stream()
                    .map(x -> x.getValue().get(0)).collect(Collectors.toList())
            : records;
    }
}