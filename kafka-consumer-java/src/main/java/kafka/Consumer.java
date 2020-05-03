package kafka;

import java.time.Duration;
import monitoring.Monitor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverRecord;
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
            .doOnError(error -> System.out.println("receiver_error!!!!!! " + error.toString()))
            .retry()
            .doOnRequest(
                requested -> {
                    System.out.println("Requested " + requested);
                }
            )
            .doOnNext(
                record -> {
                    System.out.println("New Record " + Thread.currentThread().getName());
                    Monitor.receivedRecord(record);
                }
            )
            .delayElements(Duration.ofMillis(processingDelay))
            .groupBy(record -> record.receiverOffset().topicPartition())
            .flatMap(
                partitionKey -> partitionKey
                    .publishOn(Schedulers.boundedElastic())
                    .concatMap(
                        record -> Mono
                            .fromFuture(target.call(record))
                            .doOnNext(
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
                            .thenEmpty(
                                record
                                    .receiverOffset()
                                    .commit()
                                    .onErrorContinue(
                                        CommitFailedException.class,
                                        (t, __) -> {
                                            System.out.println("onErrorContinue");
                                        }
                                    )
                            )
                    )
            );
    }
}
