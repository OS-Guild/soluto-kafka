package kafka;

import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import java.util.concurrent.TimeUnit;
import monitoring.Monitor;
import reactor.kafka.receiver.ReceiverRecord;
import target.ITarget;

public class Consumer {
    private Flowable<ReceiverRecord<String, String>> receiver;
    private final ITarget target;
    private final long processingDelay;
    private Disposable consumer;

    Consumer(Flowable<ReceiverRecord<String, String>> receiver, ITarget target, long processingDelay) {
        this.receiver = receiver;
        this.target = target;
        this.processingDelay = processingDelay;
    }

    public void start() {
        consumer =
            receiver
                .onBackpressureBuffer(Flowable.bufferSize(), false, false, () -> Monitor.backpressureBufferOverflow())
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
                        .concatMapSingle(record -> Single.create(__ -> record.receiverOffset().acknowledge()))
                )
                .subscribeOn(Schedulers.io())
                .subscribe(
                    __ -> {},
                    e -> {
                        System.out.println("error!!!! " + e.getMessage());
                    },
                    () -> {}
                );
    }

    public void stop() {
        consumer.dispose();
    }
}
