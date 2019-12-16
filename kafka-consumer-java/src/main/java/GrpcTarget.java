import java.util.Date;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import com.spotify.futures.ListenableFuturesExtra;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.function.CheckedSupplier;

public class GrpcTarget implements ITarget {
    private Channel client = ManagedChannelBuilder.forAddress(Config.TARGET_HOST, Config.TARGET_PORT).usePlaintext().build();
    private TargetRetryPolicy retryPolicy;

    public GrpcTarget(final TargetRetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public CompletableFuture<TargetResponse> call(final ConsumerRecord<String, String> record) {
        final var json = record.value();
        final var callTargetPayloadBuilder = KafkaMessage.CallTargetPayload.newBuilder();
        callTargetPayloadBuilder.setRecordOffset(record.offset());
        callTargetPayloadBuilder.setRecordTimestamp(record.timestamp());
        callTargetPayloadBuilder.setMsgJson(json);
        final CallTargetGrpc.CallTargetFutureStub futureStub = CallTargetGrpc.newFutureStub(client);

        final long startTime = (new Date()).getTime();
        CheckedSupplier<CompletionStage<KafkaMessage.CallTargetResponse>> completionStageCheckedSupplier = () -> ListenableFuturesExtra.toCompletableFuture(futureStub.callTarget(callTargetPayloadBuilder.build()));

        return Failsafe
            .with(retryPolicy.<KafkaMessage.CallTargetResponse>get(record, r -> r.getStatusCode()))
            .getStageAsync(completionStageCheckedSupplier)
            .thenApplyAsync(response -> {
                var callLatency = response.getReceivedTimestamp() == 0L ? OptionalLong.empty() : OptionalLong.of(response.getReceivedTimestamp() - startTime);
                var resultLatency = response.getCompletedTimestamp() == 0L ? OptionalLong.empty() : OptionalLong.of((new Date()).getTime() - response.getCompletedTimestamp());
                return new TargetResponse(TargetResponseType.Success, callLatency, resultLatency);
            })
            .exceptionally(TargetResponse::Error);
    }
}