import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.Date;
import java.util.OptionalLong;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.function.CheckedSupplier;
import reactor.kafka.receiver.ReceiverRecord;

public class HttpTarget implements ITarget {
    private final HttpClient client = HttpClient.newHttpClient();
    private TargetRetryPolicy retryPolicy;

    public HttpTarget(final TargetRetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public CompletableFuture<TargetResponse> call(final ReceiverRecord<String, String> record) {
        final var request = HttpRequest
            .newBuilder()
            .uri(URI.create(Config.SENDING_PROTOCOL + "://" + Config.TARGET))
            .header("Content-Type", "application/json")
            .header("x-record-topic", record.topic())
            .header("x-record-key", record.key())
            .header("x-record-partition", String.valueOf(record.partition()))
            .header("x-record-offset", String.valueOf(record.offset()))
            .header("x-record-timestamp", String.valueOf(record.timestamp()))
            .header("x-record-original-topic", this.getOriginalTopic(record))
            .POST(HttpRequest.BodyPublishers.ofString(record.value()))
            .build();

        final long startTime = (new Date()).getTime();
        final CheckedSupplier<CompletionStage<HttpResponse<String>>> completionStageCheckedSupplier =
            () -> client.sendAsync(request, HttpResponse.BodyHandlers.ofString());

        return Failsafe
            .with(retryPolicy.<HttpResponse<String>>get(record, r -> r.statusCode()))
            .getStageAsync(completionStageCheckedSupplier)
            .thenApplyAsync(
                response -> {
                    var callLatency = !response.headers().firstValueAsLong("x-received-timestamp").isPresent()
                        ? OptionalLong.empty()
                        : OptionalLong.of(
                        response.headers().firstValueAsLong("x-received-timestamp").getAsLong() - startTime
                    );
                    var resultLatency = !response.headers().firstValueAsLong("x-completed-timestamp").isPresent()
                        ? OptionalLong.empty()
                        : OptionalLong.of(
                        (new Date()).getTime() -
                            response.headers().firstValueAsLong("x-completed-timestamp").getAsLong()
                    );
                    return new TargetResponse(TargetResponseType.Success, callLatency, resultLatency);
                }
            )
            .exceptionally(TargetResponse::Error);
    }
}
