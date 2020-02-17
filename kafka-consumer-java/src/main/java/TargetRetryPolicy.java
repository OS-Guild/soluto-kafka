import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.function.ToIntFunction;
import java.util.Optional;
import net.jodah.failsafe.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TargetRetryPolicy {
    private Producer producer;
    private String retryTopic;
    private String deadLetterTopic;

    public TargetRetryPolicy(Producer producer, String retryTopic, String deadLetterTopic) {
        this.producer = producer;
        this.retryTopic = retryTopic;
        this.deadLetterTopic = deadLetterTopic;
    }

    public <T> RetryPolicy<T> get(ConsumerRecord<String, String> record, final ToIntFunction<T> getStatusCode) {
        var executionStart = new Date().getTime();
        return new RetryPolicy<T>()
            .withBackoff(10, 250, ChronoUnit.MILLIS, 5)
            .handleResultIf(r -> getStatusCode.applyAsInt(r) >= 500)
            .onSuccess(
                x -> {
                    var statusCode = getStatusCode.applyAsInt(x.getResult());

                    if (400 <= statusCode && statusCode < 500) {
                        if (deadLetterTopic != null) {
                            Monitor.processMessageFailed();
                            producer.produce("deadLetter", deadLetterTopic, record);
                            Monitor.deadLetterProcdued(record);
                        }
                        return;
                    }
                    Monitor.processMessageSuccess(executionStart);
                }
            )
            .onFailedAttempt(
                x -> Monitor.targetExecutionRetry(
                    record,
                    Optional.<String>ofNullable(String.valueOf(getStatusCode.applyAsInt(x.getLastResult()))),
                    x.getLastFailure(),
                    x.getAttemptCount()
                )
            )
            .onRetriesExceeded(
                __ -> {
                    if (retryTopic != null) {
                        producer.produce("retry", retryTopic, record);
                        Monitor.retryProduced(record);
                    }
                }
            );
    }
}
