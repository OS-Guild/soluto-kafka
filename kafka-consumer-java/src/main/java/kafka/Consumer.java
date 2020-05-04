package kafka;

import configuration.Config;
import java.time.Duration;
import monitoring.Monitor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;
import target.ITarget;

public class Consumer {
    private Flux<ReceiverRecord<String, String>> receiver;
    private final ITarget target;
    private final long processingDelay;

    Consumer(Flux<ReceiverRecord<String, String>> receiver, ITarget target, long processingDelay) {
        this.receiver = receiver;
        this.target = target;
        this.processingDelay = processingDelay;
    }

    public Flux<?> stream() {
        return receiver
            .doOnRequest(
                requested -> {
                    System.out.println("Requested " + requested);
                }
            )
            .publishOn(Schedulers.boundedElastic(), false, Config.BUFFER_SIZE)
            .doOnNext(
                record -> {
                    System.out.println("New Record " + record.partition() + " " + Thread.currentThread().getName());
                    Monitor.receivedRecord(record);
                }
            )
            .delayElements(Duration.ofMillis(processingDelay))
            .groupBy(record -> record.receiverOffset().topicPartition())
            .flatMap(
                topicPartition -> topicPartition
                    .publishOn(Schedulers.boundedElastic())
                    .flatMap(
                        record -> Mono
                            .fromFuture(target.call(record))
                            .doOnSuccess(
                                targetResponse -> {
                                    System.out.println("Http response: " + Thread.currentThread().getName());
                                    if (targetResponse.callLatency.isPresent()) {
                                        Monitor.callTargetLatency(targetResponse.callLatency.getAsLong());
                                    }
                                    if (targetResponse.resultLatency.isPresent()) {
                                        Monitor.resultTargetLatency(targetResponse.resultLatency.getAsLong());
                                    }
                                }
                            )
                            .thenEmpty(record.receiverOffset().commit())
                    ),
                Config.TARGET_CONCURRENCY
            )
            .retryWhen(
                Retry
                    .indefinitely()
                    .filter(e -> e instanceof CommitFailedException)
                    .doBeforeRetry(
                        x -> {
                            System.out.println("retry????? " + x.failure().getClass());
                        }
                    )
                    .doAfterRetry(
                        x -> {
                            System.out.println("after_retry????? " + x.failure().getClass());
                        }
                    )
            );
    }
}
