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
            .doOnNext(
                x -> {
                    if (x.callLatency.isPresent()) {
                        Monitor.callTargetLatency(x.callLatency.getAsLong());
                    }
                    if (x.resultLatency.isPresent()) {
                        Monitor.resultTargetLatency(x.resultLatency.getAsLong());
                    }
                }
            )
            .flatMap(
                targetResponse -> {
                    if (targetResponse.type == TargetResponseType.Error) {
                        if (targetResponse.exception instanceof ConnectException) {
                            Monitor.targetConnectionUnavailable();
                        } else {
                            Monitor.unexpectedError(targetResponse.exception);
                            return Flowable.error(targetResponse.exception);
                        }
                    }
                    return Flowable.just(targetResponse);
                }
            )
            .toList();
    }
}
