import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

    Flowable<ConsumerRecord<String, String>> process(
        Iterable<Iterable<ConsumerRecord<String, String>>> consumedPartitioned
    ) {
        return Flowable
            .fromIterable(consumedPartitioned)
            .delay(processingDelay, TimeUnit.MILLISECONDS)
            .flatMap(this::processPartition, Config.CONCURRENT_RECORDS);
    }

    private Flowable<ConsumerRecord<String, String>> processPartition(
        Iterable<ConsumerRecord<String, String>> partition
    ) {
        return Flowable
            .fromIterable(partition)
            .doOnNext(Monitor::messageLatency)
            .doOnNext(__ -> Monitor.processMessageStarted())
            .flatMap(record -> Flowable.fromFuture(target.call(record)), Config.CONCURRENCY_PER_PARTITION)
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
                x -> x.type == TargetResponseType.Error ? Flowable.error(x.exception.getCause()) : Flowable.empty()
            );
    }
}
