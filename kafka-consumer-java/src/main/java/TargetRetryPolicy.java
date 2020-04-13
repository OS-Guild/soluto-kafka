import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.function.ToIntFunction;
import java.util.Optional;
import java.util.regex.Pattern;
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
        var delay = Config.TARGET_RETRY_POLICY_EXPONENTIAL_BACKOFF.get(0);
        var maxDelay = Config.TARGET_RETRY_POLICY_EXPONENTIAL_BACKOFF.get(1);
        var delayFactor = Config.TARGET_RETRY_POLICY_EXPONENTIAL_BACKOFF.get(2);

        return new RetryPolicy<T>()
            .withBackoff(delay, maxDelay, ChronoUnit.MILLIS, delayFactor)
            .handleResultIf(
                r -> matches(
                    String.valueOf(getStatusCode.applyAsInt(r)),
                    Config.TARGET_RETRY_POLICY_RETRY_STATUS_CODES_REGEX
                )
            )
            .onSuccess(
                x -> {
                    var statusCode = String.valueOf(getStatusCode.applyAsInt(x.getResult()));

                    if (matches(statusCode, Config.TARGET_RETRY_POLICY_RETRY_TOPIC_STATUS_CODES_REGEX)) {
                        Monitor.processMessageError();
                        if (retryTopic != null) {
                            producer.produce("retry", retryTopic, record);
                            Monitor.retryProduced(record);
                            return;
                        }
                    }

                    if (matches(statusCode, Config.TARGET_RETRY_POLICY_DEAD_LETTER_TOPIC_STATUS_CODES_REGEX)) {
                        Monitor.processMessageError();
                        if (deadLetterTopic != null) {
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

    private boolean matches(String value, String pattern) {
        var r = Pattern.compile(pattern);
        var m = r.matcher(value);
        return m.find();
    }
}
