import io.reactivex.Flowable;
import io.reactivex.Single;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;

class Processor {
    ITarget target;
    long processingDelay;

    Processor(
        long processingDelay,
        KafkaProducer<String, String> kafkaProducer,
        String retryTopic,
        String deadLetterTopic
    ) {
        this.processingDelay = processingDelay;
        var targetRetryPolicy = new TargetRetryPolicy(new Producer(kafkaProducer), retryTopic, deadLetterTopic);
        target =
            Config.SENDING_PROTOCOL.equals("grpc") ? new GrpcTarget(targetRetryPolicy)
                : new HttpTarget(targetRetryPolicy);
    }

    Single<List<TargetResponse>> processBatch(ConsumerRecords<String, String> records) {
        return Flowable
            .fromIterable(records)
            .delay(processingDelay, TimeUnit.MILLISECONDS)
            .doOnNext(Monitor::messageLatency)
            .doOnNext(__ -> Monitor.processMessageStarted())
            .flatMap(record -> Flowable.fromFuture(target.call(record)))
            .flatMap(
                targetResponse -> targetResponse.type == TargetResponseType.Error
                    ? Flowable.error(targetResponse.exception)
                    : Flowable.just(targetResponse)
            )
            .doOnNext(
                targetResponse -> {
                    if (targetResponse.callLatency.isPresent()) {
                        Monitor.callTargetLatency(targetResponse.callLatency.getAsLong());
                    }
                    if (targetResponse.resultLatency.isPresent()) {
                        Monitor.resultTargetLatency(targetResponse.resultLatency.getAsLong());
                    }
                }
            )
            .doOnError(
                exception -> {
                    if (exception.getCause() instanceof ConnectException) {
                        Monitor.targetConnectionUnavailable();
                    } else {
                        Monitor.unexpectedError(exception);
                    }
                }
            )
            .toList();
    }
}
