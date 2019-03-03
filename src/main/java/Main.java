import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

public class Main {
    static Config config;
    static Monitor monitor;
    static KafkaCreator kafkaCreator;
    static KafkaConsumer<String, String> consumer;
    static KafkaProducer<String, String> producer;
    static HttpClient client = HttpClient.newHttpClient();
    static boolean running = true;

    public static void main(String[] args) throws Exception {
        init();
        consumer.subscribe(Collections.singletonList(config.TOPIC));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
        }));

        monitor.serviceStarted();

        try {
            while (running) {
                var consumed = consumer.poll(Duration.ofMillis(config.CONSUMER_POLL_TIMEOUT));
                if (consumed.count() == 0) continue;
                monitor.consumed(consumed);
                
                var consumedDedup = dedup(consumed);
                monitor.consumedDedup(consumed);
                
                if (config.CONCURRENCY > 1) {
                    processParallel(consumedDedup);
                }
                else {
                    processSequence(consumedDedup);
                }

                try {
                    consumer.commitSync();
                } catch (CommitFailedException ignored) {
                    monitor.commitFailed();
                }
            }
        } catch (Exception e) {
            monitor.unexpectedError(e);
        } finally {
            shutdown();
            monitor.serviceShutdown();
        }
    }

    private static void init() throws Exception {
        config = new Config();
        monitor = new Monitor(config);
        consumer = new KafkaCreator(config).createConsumer();
        producer = new KafkaCreator(config).createProducer();
    }

    private static void shutdown() {
        consumer.unsubscribe();
        consumer.close();
    }

    private static Iterable<ConsumerRecord<String, String>> dedup(ConsumerRecords<String, String> records) {
        return config.SHOULD_DEDUP_BY_KEY
            ? StreamSupport.stream(records.spliterator(), false)
                .collect(Collectors.groupingBy(ConsumerRecord::key))
                .entrySet()
                .stream()
                .map(x -> x.getValue().get(0))
                .collect(Collectors.toList())
            : records;
    }

    private static void processSequence(Iterable<ConsumerRecord<String, String>> records) throws IOException, InterruptedException, ExecutionException {
        Flowable.fromIterable(records)
            .doOnNext(record -> monitor.messageLatency(record))
            .concatMap(record -> Flowable.fromFuture(sendHttpReqeust(record)))
            .subscribeOn(Schedulers.io())
            .blockingSubscribe();
    }

    private static void processParallel(Iterable<ConsumerRecord<String, String>> records) throws IOException, InterruptedException {
        Flowable.fromIterable(records)
            .doOnNext(record -> monitor.messageLatency(record))
            .flatMap(record -> Flowable.fromFuture(sendHttpReqeust(record)))
            .subscribeOn(Schedulers.io())
            .blockingSubscribe();        
    }

    private static CompletableFuture<Boolean> sendHttpReqeust(ConsumerRecord<String, String> record) {
        var request = HttpRequest
            .newBuilder()
            .uri(URI.create(config.TARGET_ENDPOINT))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(record.value()))
            .build();

        var executionStart = new Date().getTime();
        return client
            .sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApplyAsync(response -> {
                if (response.statusCode() == 500) {
                    produceDeadLetter(record);
                }
                else {
                    monitor.processCompleted(executionStart);                    
                }
                return true;                            
            })
            .exceptionally(exception -> {
                monitor.sendHttpReqeustError(record, exception);
                produceDeadLetter(record);
                return true;
            });
    }

    private static void produceDeadLetter(ConsumerRecord<String, String> record) {
        producer.send(new ProducerRecord<>(config.DEAD_LETTER_TOPIC, record.key().toString(), record.value().toString()), (metadata, err) -> {
            if (err != null) {
                monitor.deadLetterProduceError(record ,err);
                return;
            }
            monitor.deadLetterProduce();
        });
    }
}
