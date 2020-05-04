package kafka;

import configuration.Config;
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
        var scheduler = Schedulers.boundedElastic();
        return receiver
            .doOnRequest(
                requested -> {
                    System.out.println("Requested " + requested);
                }
            )
            .publishOn(scheduler, false, Config.BUFFER_SIZE)
            .doOnNext(
                record -> {
                    System.out.println("New Record " + record.partition() + Thread.currentThread().getName());
                    Monitor.receivedRecord(record);
                }
            )
            .delayElements(Duration.ofMillis(processingDelay))
            .groupBy(record -> record.receiverOffset().topicPartition() + "_" + record.key()) //is this a problem?
            .publishOn(scheduler)
            .flatMap(
                topicPartition -> topicPartition
                    .publishOn(scheduler)
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
                            .thenEmpty(record.receiverOffset().commit())
                    )
            )
            .onErrorContinue(
                a -> a instanceof CommitFailedException,
                (a, v) -> {
                    System.out.println("commit_failed");
                }
            );
    }
}
