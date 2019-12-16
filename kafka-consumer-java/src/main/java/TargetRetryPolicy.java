import java.util.function.ToIntFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import net.jodah.failsafe.RetryPolicy;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;

public class TargetRetryPolicy {

    private ErrorProducer errorProducer;

    public TargetRetryPolicy(ErrorProducer errorProducer) {
        this.errorProducer = errorProducer;
    }

    public <T> RetryPolicy<T> get(ConsumerRecord<String, String> record, final ToIntFunction<T> getStatusCode) {
        var executionStart = new Date().getTime();
        return new RetryPolicy<T>()
            .withBackoff(10, 250, ChronoUnit.MILLIS, 5)
            .handleResultIf(r -> getStatusCode.applyAsInt(r) >= 500)
            .onSuccess(x -> {
                var statusCode = getStatusCode.applyAsInt(x.getResult());

                if (400 <= statusCode && statusCode < 500) {
                    if (Config.DEAD_LETTER_TOPIC != null) {
                        errorProducer.produce("deadLetter", Config.DEAD_LETTER_TOPIC, record);
                    }
                    return;
                }
                Monitor.processMessageCompleted(executionStart);
            })
            .onFailedAttempt(x -> Monitor.targetExecutionRetry(record, Optional.<String>ofNullable(String.valueOf(getStatusCode.applyAsInt(x.getLastResult()))), x.getLastFailure(), x.getAttemptCount()))
            .onRetriesExceeded(__ -> {
                if (Config.RETRY_TOPIC != null) {
                    errorProducer.produce("retry", Config.RETRY_TOPIC, record);
                }
            });
    }
}