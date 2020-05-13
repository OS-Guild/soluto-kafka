package kafka;

import configuration.Config;
import java.time.Duration;
import monitoring.Monitor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
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
            .doOnNext(records -> System.out.println(String.format("processing batch of %s records", records.count())))
            .concatMap(
                records -> Flux
                    .fromIterable(records)
                    .groupBy(x -> x.partition())
                    .delayElements(Duration.ofMillis(Config.PROCESSING_DELAY))
                    .publishOn(Schedulers.parallel())
                    .flatMap(
                        partition -> partition
                            .doOnNext(record -> Monitor.receivedRecord(record))
                            .concatMap(
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
                    .collectList()
            )
            .map(
                __ -> {
                    kafkaConsumer.commit();
                    return 0;
                }
            )
            .doOnNext(__ -> kafkaConsumer.poll())
            .doOnSubscribe(__ -> kafkaConsumer.poll())
            .onErrorContinue(
                a -> a instanceof CommitFailedException || a instanceof RetriableCommitFailedException,
                (a, v) -> {}
            );
    }
}
