import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
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
                if (ready == false && consumer.assignment().size() > 0) {
                    ready = true;
                    Monitor.consumerReady(id);
                }                
                if (consumed.count() == 0) continue;
                Monitor.consumed(consumed);
                
                var consumedDedup = dedup(consumed);
                Monitor.consumedDedup(consumed);
                
                process(consumedDedup);

                try {
                    consumer.commitSync();
                } catch (CommitFailedException ignored) {
                    Monitor.commitFailed();
                }
            }
        } catch (Exception e) {
            if (isTargetUnavailableException(e)) {
                Monitor.targetUnavailable();
            }
            else {
                Monitor.unexpectedError(e);
            }
        } finally {
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

    private void process(Iterable<ConsumerRecord<String, String>> records) throws IOException, InterruptedException {
        Flowable.fromIterable(records)
            .doOnNext(record -> Monitor.messageLatency(record))
            .flatMap(record -> Flowable.fromFuture(sendHttpReqeust(record)), Config.CONCURRENCY)                  
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

         var retryPolicy = new RetryPolicy<HttpResponse<String>>()
            .withBackoff(10, 250, ChronoUnit.MILLIS, 5)
            .abortOn(ConnectException.class)
            .handleResultIf(r -> r.statusCode() >= 500)
            .onSuccess(x -> Monitor.processCompleted(executionStart))
            .onFailedAttempt(x -> Monitor.targetExecutionRetry(record, x.getLastFailure(), x.getAttemptCount()))            
            .onRetriesExceeded(__ -> produceDeadLetter(record));

        return Failsafe
            .with(retryPolicy)
            .getStageAsync(() -> client.sendAsync(request, HttpResponse.BodyHandlers.ofString()))
            .thenApplyAsync(__ -> true)
            .exceptionally(throwable -> {
                if (throwable.getCause() instanceof ConnectException) {
                    throw new RuntimeException(throwable);
                }
                return true;
            });
    }

    private void produceDeadLetter(ConsumerRecord<String, String> record) {
        producer.send(new ProducerRecord<>(Config.DEAD_LETTER_TOPIC, record.key().toString(), record.value().toString()), (metadata, err) -> {
            if (err != null) {
                Monitor.deadLetterProduceError(record ,err);
                return;
            }
            Monitor.deadLetterProduced(record);
        });
    }

    private boolean isTargetUnavailableException(Exception exception) {
        if (exception.getCause().getCause().getCause().getCause() instanceof ConnectException) {
            return true;
        }
        return false;
    }
}
