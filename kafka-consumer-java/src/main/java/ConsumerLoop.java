import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class ConsumerLoop implements Runnable, IReady {
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
                if (!ready && consumer.assignment().size() > 0) {
                    ready = true;
                    Monitor.consumerReady(id);
                }
                if (consumed.count() == 0) continue;
                Monitor.consumed(consumed);

                var consumedPartitioned = partitionByKey(consumed);
                Monitor.consumedPartitioned(consumedPartitioned);

                process(consumedPartitioned);

                try {
                    consumer.commitSync();
                } catch (CommitFailedException ignored) {
                    Monitor.commitFailed();
                }
            }
        } catch (Exception e) {
            if (e.getCause() instanceof ConnectException) {
                Monitor.targetConnectionUnavailable();
            } else {
                Monitor.unexpectedError(e);
            }
        } finally {
            ready = false;
            consumer.unsubscribe();
            consumer.close();
        }
    }

    public void stop() {
        running = false;
    }

    @Override
    public boolean ready() {
        return ready;
    }

    private Iterable<Iterable<ConsumerRecord<String, String>>> partitionByKey(ConsumerRecords<String, String> records) {
        return StreamSupport.stream(records.spliterator(), false)
                .collect(Collectors.groupingBy(ConsumerRecord::key))
                .values()
                .stream()
                .map(this::createPartition)
                .collect(Collectors.toList());
    }

    private List<ConsumerRecord<String, String>> createPartition(List<ConsumerRecord<String, String>> consumerRecords) {
            List<ConsumerRecord<String, String>> sorted = consumerRecords
                    .stream()
                    .sorted(Comparator.comparingLong(ConsumerRecord::offset))
                    .collect(Collectors.toList());

            return Config.DEDUP_PARTITION_BY_KEY ? Collections.singletonList(sorted.get(0)) : sorted;
    }

    private void process(Iterable<Iterable<ConsumerRecord<String, String>>> partitions) throws IOException, InterruptedException {
        Thread.sleep(Config.PROCESSING_DELAY);        
        Flowable.fromIterable(partitions)
                .flatMap(this::processPartition, Config.CONCURRENCY)
                .subscribeOn(Schedulers.io())
                .blockingSubscribe();
    }

    private Flowable processPartition(Iterable<ConsumerRecord<String, String>> partition) {
        return Flowable.fromIterable(partition)
                .doOnNext(Monitor::messageLatency)
                .flatMap(record -> Flowable.fromFuture(callTarget(record)), Config.CONCURRENCY_PER_PARTITION)
                .flatMap(x -> x.type == TargetResponseType.Error ? Flowable.error(x.exception.getCause()) : Flowable.empty());
    }

    private CompletableFuture<TargetResponse> callTarget(ConsumerRecord<String, String> record) {        
        var request = HttpRequest
                .newBuilder()
                .uri(URI.create(Config.TARGET_ENDPOINT))
                .header("content-type", "application/json")
                .header("x-api-client", "kafka-consumer-"+id+"-"+Config.TOPIC+"-"+Config.GROUP_ID)
                .header("x-timestamp", Long.toString(record.timestamp()))
                .POST(HttpRequest.BodyPublishers.ofString(record.value()))
                .build();

        var executionStart = new Date().getTime();

        var retryPolicy = new RetryPolicy<HttpResponse<String>>()
                .withBackoff(10, 250, ChronoUnit.MILLIS, 5)
                .abortOn(ConnectException.class)
                .handleResultIf(r -> r.statusCode() >= 500)
                .onSuccess(x -> {
                    var statusCode = x.getResult().statusCode();

                    if (400 <= statusCode && statusCode < 500) {
                        produce("poison", Config.POISON_MESSAGE_TOPIC, record);
                        return;
                    }

                    Monitor.processCompleted(executionStart);
                })
                .onFailedAttempt(x -> Monitor.targetExecutionRetry(record, x.getLastResult(), x.getLastFailure(), x.getAttemptCount()))
                .onRetriesExceeded(__ -> produce("retry", Config.RETRY_TOPIC, record));

        return Failsafe
                .with(retryPolicy)
                .getStageAsync(() -> client.sendAsync(request, HttpResponse.BodyHandlers.ofString()))
                .thenApplyAsync(__ -> TargetResponse.Success)
                .exceptionally(TargetResponse::Error);
    }

    private void produce(String topicPrefix, String topic, ConsumerRecord<String, String> record) {
        producer.send(new ProducerRecord<>(topic, record.key(), record.value()), (metadata, err) -> {
            if (err != null) {
                Monitor.produceError(topicPrefix, record, err);
                return;
            }

            Monitor.topicProduced(topicPrefix, record);
        });
    }
}
