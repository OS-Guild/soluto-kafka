import com.spotify.futures.ListenableFuturesExtra;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.ToIntFunction;

class Processor {
    private HttpClient httpClient;
    private Channel grpcChannel;
    private KafkaProducer<String, String> producer;
    
    Processor(KafkaProducer<String, String> producer) {
        if (Config.SENDING_PROTOCOL.equals("grpc")) {
            this.grpcChannel = ManagedChannelBuilder.forAddress(Config.TARGET_GRPC_HOST, Config.TARGET_GRPC_PORT).usePlaintext().build();
        }
        else {
            this.httpClient = HttpClient.newHttpClient();
        }
        this.producer = producer;
    }

    void process(Iterable<Iterable<ConsumerRecord<String, String>>> partitions) throws IOException, InterruptedException {
        Thread.sleep(Config.PROCESSING_DELAY);
        Flowable.fromIterable(partitions)
                .flatMap(this::processPartition, Config.CONCURRENCY)
                .subscribeOn(Schedulers.io())
                .blockingSubscribe();
    }

    private <T> RetryPolicy<T> getRetryPolicy(ConsumerRecord<String, String> record, final ToIntFunction<T> getStatusCode) {
        var executionStart = new Date().getTime();

        return new RetryPolicy<T>()
                .withBackoff(10, 250, ChronoUnit.MILLIS, 5)
                .handleResultIf(r -> getStatusCode.applyAsInt(r) >= 500)
                .onSuccess(x -> {
                    var statusCode = getStatusCode.applyAsInt(x.getResult());

                    if (400 <= statusCode && statusCode < 500) {
                        if (Config.DEAD_LETTER_TOPIC != null) {
                            produce("deadLetter", Config.DEAD_LETTER_TOPIC, record);
                        }
                        return;
                    }
                    Monitor.processMessageCompleted(executionStart);
                })
                .onFailedAttempt(x -> Monitor.targetExecutionRetry(record, Optional.<String>ofNullable(String.valueOf(getStatusCode.applyAsInt(x.getLastResult()))), x.getLastFailure(), x.getAttemptCount()))
                .onRetriesExceeded(__ -> {
                    if (Config.RETRY_TOPIC != null) {
                        produce("retry", Config.RETRY_TOPIC, record);
                    }
                });
    }

    private CompletableFuture<TargetResponse> callHttpTarget(ConsumerRecord<String, String> record) {
        var request = HttpRequest
                .newBuilder()
                .uri(URI.create("http://" + Config.TARGET_HTTP_HOST + ":" + Config.TARGET_HTTP_PORT))
                .header("Content-Type", "application/json")
                .header("x-record-offset", String.valueOf(record.offset()))
                .header("x-record-timestamp", String.valueOf(record.timestamp()))
                .POST(HttpRequest.BodyPublishers.ofString(record.value()))
                .build();

        final long startTime = (new Date()).getTime();
        var retryPolicy = this.<HttpResponse<String>>getRetryPolicy(record, r -> r.statusCode());

        CheckedSupplier<CompletionStage<HttpResponse<String>>> completionStageCheckedSupplier = () -> httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        
        return Failsafe
                .with(retryPolicy)
                .getStageAsync(completionStageCheckedSupplier)
                .thenApplyAsync(response -> {
                    var callLatency = !response.headers().firstValueAsLong("x-received-timestamp").isPresent() ? OptionalLong.empty() : OptionalLong.of(response.headers().firstValueAsLong("x-received-timestamp").getAsLong() - startTime) ;
                    var resultLatency = !response.headers().firstValueAsLong("x-completed-timestamp").isPresent() ? OptionalLong.empty() : OptionalLong.of((new Date()).getTime() - response.headers().firstValueAsLong("x-completed-timestamp").getAsLong()) ;
                    return new TargetResponse(TargetResponseType.Success, callLatency, resultLatency);
                })
                .exceptionally(TargetResponse::Error);
    }

    private CompletableFuture<TargetResponse> callGrpcTarget(ConsumerRecord<String, String> record) {
        final var json = record.value();
        final var callTargetPayloadBuilder = KafkaMessage.CallTargetPayload.newBuilder();
        callTargetPayloadBuilder.setRecordOffset(record.offset());
        callTargetPayloadBuilder.setRecordTimestamp(record.timestamp());
        callTargetPayloadBuilder.setMsgJson(json);
        final CallTargetGrpc.CallTargetFutureStub futureStub = CallTargetGrpc.newFutureStub(grpcChannel);

        final long startTime = (new Date()).getTime();
        final var retryPolicy = this.<KafkaMessage.CallTargetResponse>getRetryPolicy(record, r -> r.getStatusCode());
        CheckedSupplier<CompletionStage<KafkaMessage.CallTargetResponse>> completionStageCheckedSupplier = () -> ListenableFuturesExtra.toCompletableFuture(futureStub.callTarget(callTargetPayloadBuilder.build()));

        return Failsafe
                .with(retryPolicy)
                .getStageAsync(completionStageCheckedSupplier)
                .thenApplyAsync(response -> {
                    var callLatency = response.getReceivedTimestamp() == 0L ? OptionalLong.empty() : OptionalLong.of(response.getReceivedTimestamp() - startTime);
                    var resultLatency = response.getCompletedTimestamp() == 0L ? OptionalLong.empty() : OptionalLong.of((new Date()).getTime() - response.getCompletedTimestamp());
                    return new TargetResponse(TargetResponseType.Success, callLatency, resultLatency);
                })
                .exceptionally(TargetResponse::Error);
    }

    private CompletableFuture<TargetResponse> callTarget(ConsumerRecord<String, String> record) {
        if (grpcChannel != null) {
            return callGrpcTarget(record);
        }
        return callHttpTarget(record);
    }

    private Flowable processPartition(Iterable<ConsumerRecord<String, String>> partition) {
        return Flowable.fromIterable(partition)
                .doOnNext(Monitor::messageLatency)
                .flatMap(record -> Flowable.fromFuture(callTarget(record)), Config.CONCURRENCY_PER_PARTITION)
                .doOnNext(x -> {
                    if (x.callLatency.isPresent()) {
                        Monitor.callTargetLatency(x.callLatency.getAsLong());
                    }
                    if (x.resultLatency.isPresent()) {
                        Monitor.resultTargetLatency(x.resultLatency.getAsLong());
                    }                    
                })
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
