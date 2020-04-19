package kafka;

import configuration.Config;
import io.reactivex.*;
import io.reactivex.schedulers.Schedulers;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import monitoring.Monitor;
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

    public Flowable<Void> stream() {
        return receiver
            .observeOn(Schedulers.io(), false, Config.BUFFER_SIZE)
            .doOnNext(record -> Monitor.receivedRecord(record))
            .delay(processingDelay, TimeUnit.MILLISECONDS)
            .groupBy(record -> record.partition())
            .flatMap(
                partition -> partition
                    .concatMap(
                        record -> Flowable
                            .fromFuture(target.call(record))
                            .doOnNext(
                                targetResponse -> {
                                    System.out.println("partition is " + record.partition());
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
                    .sample(500, TimeUnit.MILLISECONDS)
                    .concatMap(record -> record.receiverOffset().commit())
            )
            .subscribeOn(Schedulers.io());
    }
}
