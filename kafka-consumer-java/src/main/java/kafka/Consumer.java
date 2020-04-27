package kafka;

import configuration.Config;
import io.reactivex.*;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import monitoring.Monitor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.reactivestreams.Publisher;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import target.ITarget;

public class Consumer {
    private Flowable<ReceiverRecord<String, String>> receiver;
    private final ITarget target;
    private final long processingDelay;

    Consumer(Flowable<ReceiverRecord<String, String>> receiver, ITarget target, long processingDelay) {
        this.receiver = receiver;
        this.target = target;
        this.processingDelay = processingDelay;
    }

    public Flowable<?> stream() {
        return receiver
            .doOnRequest(
                requested -> {
                    System.out.println("Requested " + requested);
                }
            )
            .observeOn(Schedulers.io(), false, Config.BUFFER_SIZE)
            .doOnNext(
                record -> {
                    System.out.println("New Record " + Thread.currentThread().getName());

                    Monitor.receivedRecord(record);
                }
            )
            .delay(processingDelay, TimeUnit.MILLISECONDS)
            .groupBy(record -> record.partition())
            .flatMap(
                partition -> partition
                    .observeOn(Schedulers.io())
                    .concatMap(
                        record -> Flowable
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
                            .map(__ -> record)
                    )
                    .sample(1000, TimeUnit.MILLISECONDS)
                    .concatMap(record -> record.receiverOffset().commit())
            );
    }
}
