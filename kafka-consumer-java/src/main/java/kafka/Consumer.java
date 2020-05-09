package kafka;

import configuration.Config;
import java.time.Duration;
import monitoring.Monitor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import target.ITarget;

public class Consumer {
    private ReactiveKafkaClient<String, String> kafkaConsumer;
    private final ITarget target;

    public Consumer(ReactiveKafkaClient<String, String> kafkaConsumer, ITarget target) {
        this.kafkaConsumer = kafkaConsumer;
        this.target = target;
    }

    public Flux<?> stream() {
        return kafkaConsumer
            .flatMapIterable(records -> records)
            .doOnNext(record -> Monitor.receivedRecord(record))
            .groupBy(x -> x.partition())
            .delayElements(Duration.ofMillis(Config.PROCESSING_DELAY))
            .publishOn(Schedulers.parallel())
            .flatMap(
                partition -> partition.concatMap(
                    record -> Mono
                        .fromFuture(target.call(record))
                        .doOnSuccess(
                            targetResponse -> {
                                if (targetResponse.callLatency.isPresent()) {
                                    Monitor.callTargetLatency(targetResponse.callLatency.getAsLong());
                                }
                                if (targetResponse.resultLatency.isPresent()) {
                                    Monitor.resultTargetLatency(targetResponse.resultLatency.getAsLong());
                                }
                            }
                        )
                )
            )
            .onBackpressureBuffer()
            .doOnRequest(kafkaConsumer::poll)
            .limitRate(Config.MAX_POLL_RECORDS)
            .sample(Duration.ofMillis(Config.COMMIT_INTERVAL))
            .concatMap(
                __ -> {
                    kafkaConsumer.commit();
                    return Mono.empty();
                }
            )
            .onErrorContinue(a -> a instanceof CommitFailedException, (a, v) -> {});
    }
}
