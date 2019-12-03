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
    private HttpClient client = HttpClient.newHttpClient();
    private KafkaProducer<String, String> producer;
    Channel channel = ManagedChannelBuilder.forAddress(Config.GRPC_HOST, Config.GRPC_PORT).usePlaintext().build();
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

    private <T> RetryPolicy<T> getRetryPolicy(ConsumerRecord<String, String> record, final ToIntFunction<T> getStatusCode) {
        var executionStart = new Date().getTime();

        return new RetryPolicy<T>()
                .withBackoff(10, 250, ChronoUnit.MILLIS, 5)
                .handleResultIf(r -> getStatusCode.applyAsInt(r) >= 500)
                .onSuccess(x -> {
                    var statusCode = getStatusCode.applyAsInt(x.getResult());

                    if (400 <= statusCode && statusCode < 500) {
                        produce("poison", Config.POISON_MESSAGE_TOPIC, record);
                        return;
                    }
                    Monitor.processMessageCompleted(executionStart);
                })
                .onFailedAttempt(x -> Monitor.targetExecutionRetry(record, Optional.<String>ofNullable(String.valueOf(getStatusCode.applyAsInt(x.getLastResult()))), x.getLastFailure(), x.getAttemptCount()))
                .onRetriesExceeded(__ -> produce("retry", Config.RETRY_TOPIC, record));
    }

    private CompletableFuture<TargetResponse> callHttpTarget(ConsumerRecord<String, String> record) {
        var request = HttpRequest
                .newBuilder()
                .uri(URI.create(Config.TARGET_ENDPOINT))
                .header("Content-Type", "application/json")
                .header("x-record-offset", String.valueOf(record.offset()))
                .header("x-record-timestamp", String.valueOf(record.timestamp()))
                .header("x-record-target-send-timestamp", String.valueOf(System.currentTimeMillis()))
                .POST(HttpRequest.BodyPublishers.ofString(record.value()))
                .build();

        var retryPolicy = this.<HttpResponse<String>>getRetryPolicy(record, r -> r.statusCode());

        CheckedSupplier<CompletionStage<HttpResponse<String>>> completionStageCheckedSupplier = () -> client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        
        return Failsafe
                .with(retryPolicy)
                .getStageAsync(completionStageCheckedSupplier)
                .thenApplyAsync(response -> new TargetResponse(TargetResponseType.Success, response.headers().firstValueAsLong("x-call-target-latency")))
                .exceptionally(TargetResponse::Error);
    }

    private CompletableFuture<TargetResponse> callGrpcTarget(ConsumerRecord<String, String> record) {
        final var json = record.value();
        final var callTargetPayloadBuilder = KafkaMessage.CallTargetPayload.newBuilder();
        callTargetPayloadBuilder.setRecordOffset(record.offset());
        callTargetPayloadBuilder.setRecordTimestamp(record.timestamp());
        callTargetPayloadBuilder.setRecordTargetSendTimestamp(System.currentTimeMillis());
        callTargetPayloadBuilder.setMsgJson(json);
        final CallTargetGrpc.CallTargetFutureStub futureStub = CallTargetGrpc.newFutureStub(channel);

        final var retryPolicy = this.<KafkaMessage.CallTargetResponse>getRetryPolicy(record, r->r.getStatusCode());
        CheckedSupplier<CompletionStage<KafkaMessage.CallTargetResponse>> completionStageCheckedSupplier = () -> ListenableFuturesExtra.toCompletableFuture(futureStub.callTarget(callTargetPayloadBuilder.build()));

        return Failsafe
                .with(retryPolicy)
                .getStageAsync(completionStageCheckedSupplier)
                .thenApplyAsync(response -> new TargetResponse(TargetResponseType.Success, response.getCallLatency() == 0L ? OptionalLong.empty() : OptionalLong.of(response.getCallLatency())))
                .exceptionally(TargetResponse::Error);
    }

    private CompletableFuture<TargetResponse> callTarget(ConsumerRecord<String, String> record) {
        if (Config.SENDING_PROTOCOL.equals("grpc")) return callGrpcTarget(record);
        else if (Config.SENDING_PROTOCOL.equals("http")) return callHttpTarget(record);
        CompletableFuture<TargetResponse> notSupportedFuture = new CompletableFuture<>();
        final TargetResponse notSupportedResponse = TargetResponse.Error(new UnsupportedOperationException());
        notSupportedFuture.complete(notSupportedResponse);
        return notSupportedFuture;
    }

    private Flowable processPartition(Iterable<ConsumerRecord<String, String>> partition) {
        return Flowable.fromIterable(partition)
                .doOnNext(Monitor::messageLatency)
                .flatMap(record -> Flowable.fromFuture(callTarget(record)), Config.CONCURRENCY_PER_PARTITION)
                .flatMap(targetResponse -> {
                    if (targetResponse.type == TargetResponseType.Error) {
                        return Flowable.error(targetResponse.exception.getCause());
                    }
                    if (targetResponse.callLatency.isPresent()) {
                        Monitor.callTargetLatency(targetResponse.callLatency.getAsLong());
                    }
                    return Flowable.empty();
                });
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
