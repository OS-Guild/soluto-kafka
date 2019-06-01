import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

class Processor {
    private HttpClient client = HttpClient.newHttpClient();
    private KafkaProducer<String, String> producer;

    Processor(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    void process(Iterable<Iterable<ConsumerRecord<String, String>>> partitions) throws IOException, InterruptedException {
        Thread.sleep(Config.PROCESSING_DELAY);
        Flowable.fromIterable(partitions)
                .flatMap(this::processPartition, Config.CONCURRENCY)
                .subscribeOn(Schedulers.io())
                .blockingSubscribe();
    }

    private CompletableFuture<TargetResponse> callTarget(ConsumerRecord<String, String> record) {
        var request = HttpRequest
                .newBuilder()
                .uri(URI.create(Config.TARGET_ENDPOINT))
                .header("Content-Type", "application/json")
                .header("x-record-offset", String.valueOf(record.offset()))
                .header("x-record-timestamp", String.valueOf(record.timestamp()))
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

    private Flowable processPartition(Iterable<ConsumerRecord<String, String>> partition) {
        return Flowable.fromIterable(partition)
                .doOnNext(Monitor::messageLatency)
                .flatMap(record -> Flowable.fromFuture(callTarget(record)), Config.CONCURRENCY_PER_PARTITION)
                .flatMap(x -> x.type == TargetResponseType.Error ? Flowable.error(x.exception.getCause()) : Flowable.empty());
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
