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

public class ConsumerLoop implements Runnable {
    private HttpClient client = HttpClient.newHttpClient();
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private boolean running;
    private boolean ready;
    private int id;

    ConsumerLoop(int id, KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer) {
        this.id = id;
        this.consumer = consumer;
        this.producer = producer;
    }

    @Override
    public void run() {
        running = true;
        consumer.subscribe(Collections.singletonList(Config.TOPIC));
        try {
            while (running) {
                var consumed = consumer.poll(Duration.ofMillis(Config.CONSUMER_POLL_TIMEOUT));
                if (consumed.count() == 0) continue;
                Monitor.consumed(consumed);
                
                var consumedDedup = dedup(consumed);
                Monitor.consumedDedup(consumed);
                
                if (Config.CONCURRENCY > 1) {
                    processParallel(consumedDedup);
                }
                else {
                    processSequence(consumedDedup);
                }

                try {
                    consumer.commitSync();
                } catch (CommitFailedException ignored) {
                    Monitor.commitFailed();
                }
            }
        } catch (Exception e) {
            Monitor.unexpectedError(e);
        } finally {
            consumer.unsubscribe();
            consumer.close();
        }
    }

    public void stop() {
        running = false;
    }

    private Iterable<ConsumerRecord<String, String>> dedup(ConsumerRecords<String, String> records) {
        return Config.SHOULD_DEDUP_BY_KEY
            ? StreamSupport.stream(records.spliterator(), false)
                .collect(Collectors.groupingBy(ConsumerRecord::key))
                .entrySet()
                .stream()
                .map(x -> x.getValue().get(0))
                .collect(Collectors.toList())
            : records;
    }

    private void processSequence(Iterable<ConsumerRecord<String, String>> records) throws IOException, InterruptedException, ExecutionException {
        Flowable.fromIterable(records)
            .doOnNext(record -> Monitor.messageLatency(record))
            .concatMap(record -> Flowable.fromFuture(sendHttpReqeust(record)))
            .subscribeOn(Schedulers.io())
            .blockingSubscribe();
    }

    private void processParallel(Iterable<ConsumerRecord<String, String>> records) throws IOException, InterruptedException {
        Flowable.fromIterable(records)
            .doOnNext(record -> Monitor.messageLatency(record))
            .flatMap(record -> Flowable.fromFuture(sendHttpReqeust(record)))
            .subscribeOn(Schedulers.io())
            .blockingSubscribe();        
    }

    private CompletableFuture<Boolean> sendHttpReqeust(ConsumerRecord<String, String> record) {
        var request = HttpRequest
            .newBuilder()
            .uri(URI.create(Config.TARGET_ENDPOINT))
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
                    Monitor.processCompleted(executionStart);                    
                }
                return true;                            
            })
            .exceptionally(exception -> {
                Monitor.sendHttpReqeustError(record, exception);
                produceDeadLetter(record);
                return true;
            });
    }

    private void produceDeadLetter(ConsumerRecord<String, String> record) {
        producer.send(new ProducerRecord<>(Config.DEAD_LETTER_TOPIC, record.key().toString(), record.value().toString()), (metadata, err) -> {
            if (err != null) {
                Monitor.deadLetterProduceError(record ,err);
                return;
            }
            Monitor.deadLetterProduce();
        });
    }
}
