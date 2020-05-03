package kafka;

import java.time.Duration;
import monitoring.Monitor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
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
        Scheduler scheduler = Schedulers.boundedElastic();
        return receiver
            .publishOn(scheduler)
            .doOnError(
                error -> {
                    System.out.println("receiver_error!!!!!! " + error.toString());
                    try {
                        Thread.sleep(1000); // Make it configurable
                    } catch (InterruptedException e) {
                        // Causes the restart
                        e.printStackTrace();
                    }
                }
            )
            //                .onErrorContinue(
            //                        CommitFailedException.class,
            //                        (t, __) -> {
            //                            System.out.println("onErrorContinue");
            //                        }
            //                )
            .retryWhen(Retry.indefinitely().filter(e -> e instanceof CommitFailedException))
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
                    .publishOn(scheduler)
                    .concatMap(
                        record -> Mono.fromFuture(target.call(record)).thenEmpty(record.receiverOffset().commit())
                    // .doOnNext(
                    //     targetResponse -> {
                    //         System.out.println("Http response: " + Thread.currentThread().getName());
                    //         if (targetResponse.callLatency.isPresent()) {
                    //             Monitor.callTargetLatency(targetResponse.callLatency.getAsLong());
                    //         }
                    //         if (targetResponse.resultLatency.isPresent()) {
                    //             Monitor.resultTargetLatency(targetResponse.resultLatency.getAsLong());
                    //         }
                    //     }
                    // )
                    // .map(__ -> record)
                    )
            // .concatMap(
            //     record -> Mono.fromCallable(
            //         () -> {
            //             System.out.println("Commit " + Thread.currentThread().getName());
            //             record.receiverOffset().acknowledge();
            //             return true;
            //         }
            //     )
            // )
            );
    }
}
