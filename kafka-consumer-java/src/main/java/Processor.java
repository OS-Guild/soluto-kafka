import com.google.gson.Gson;
import com.spotify.futures.ListenableFuturesExtra;
import io.grpc.*;
import io.grpc.stub.ClientCalls;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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

    private CompletableFuture<TargetResponse> callHttpTarget(ConsumerRecord<String, String> record) {
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
                .onFailedAttempt(x -> Monitor.targetExecutionRetry(record, Optional.<String>ofNullable(x.getLastResult().body()), x.getLastFailure(), x.getAttemptCount()))
                .onRetriesExceeded(__ -> produce("retry", Config.RETRY_TOPIC, record));

        CheckedSupplier<CompletionStage<HttpResponse<String>>> completionStageCheckedSupplier = () -> client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        return Failsafe
                .with(retryPolicy)
                .getStageAsync(completionStageCheckedSupplier)
                .thenApplyAsync(__ -> TargetResponse.Success)
                .exceptionally(TargetResponse::Error);
    }

    static final class CreateRequest {
        byte[] key;
        byte[] value;
    }

    static final class CreateResponse {
        int statusCode;
    }

    static <T> MethodDescriptor.Marshaller<T> marshallerFor(Class<T> clz) {
        Gson gson = new Gson();
        return new MethodDescriptor.Marshaller<T>() {
            @Override
            public InputStream stream(T value) {
                return new ByteArrayInputStream(gson.toJson(value, clz).getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public T parse(InputStream stream) {
                return gson.fromJson(new InputStreamReader(stream, StandardCharsets.UTF_8), clz);
            }
        };
    }

    static final MethodDescriptor<CreateRequest, CreateResponse> CREATE_METHOD =
            MethodDescriptor.newBuilder(
                    marshallerFor(CreateRequest.class),
                    marshallerFor(CreateResponse.class))
                    .setFullMethodName(
                            MethodDescriptor.generateFullMethodName("kafka-consumer", "Create"))
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .build();

    private CompletableFuture<TargetResponse> callGrpcTarget(ConsumerRecord<String, String> record) {
        Channel chan = ManagedChannelBuilder.forAddress(Config.GRPC_HOST, Config.GRPC_PORT).build();
        ClientCall<CreateRequest, CreateResponse> call =
                chan.newCall(CREATE_METHOD, CallOptions.DEFAULT);
        CreateRequest req = new CreateRequest();
        req.key = record.key().getBytes();
        req.value = record.value().getBytes();

        var executionStart = new Date().getTime();

        var retryPolicy = new RetryPolicy<CreateResponse>()
                .withBackoff(10, 250, ChronoUnit.MILLIS, 5)
                .abortOn(ConnectException.class)
                .handleResultIf(r -> r.statusCode != 0)
                .onSuccess(x -> {
                    var statusCode = x.getResult().statusCode;

                    if (statusCode != 0) {
                        produce("poison", Config.POISON_MESSAGE_TOPIC, record);
                        return;
                    }

                    Monitor.processCompleted(executionStart);
                })
                .onFailedAttempt(x -> Monitor.targetExecutionRetry(record, Optional.<String>ofNullable("grpc response code: " + x.getLastResult().statusCode) , x.getLastFailure(), x.getAttemptCount()))
                .onRetriesExceeded(__ -> produce("retry", Config.RETRY_TOPIC, record));

        CheckedSupplier<CompletableFuture<CreateResponse>> completionStageCheckedSupplier = () -> ListenableFuturesExtra.<CreateResponse>toCompletableFuture(ClientCalls.<CreateRequest, CreateResponse>futureUnaryCall(call, req));
        return Failsafe
                .with(retryPolicy)
                .<CreateResponse>getStageAsync(completionStageCheckedSupplier)
                .thenApplyAsync(__ -> TargetResponse.Success)
                .exceptionally(TargetResponse::Error);
    }


    private Flowable processPartition(Iterable<ConsumerRecord<String, String>> partition) {
        return Flowable.fromIterable(partition)
                .doOnNext(Monitor::messageLatency)
                .flatMap(record -> {
                    final CompletableFuture<TargetResponse> future = Config.USE_GRPC ? callGrpcTarget(record) : callHttpTarget(record);
                    return Flowable.fromFuture(future);
                }, Config.CONCURRENCY_PER_PARTITION)
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
