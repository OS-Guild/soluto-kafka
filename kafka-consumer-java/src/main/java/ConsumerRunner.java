import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ConsumerRunner implements IReady {
    private final Consumer<String, String> kafkaConsumer;
    private final List<String> topics;
    private final Processor processor;
    private Disposable consumer;
    private boolean ready;

    ConsumerRunner(Consumer<String, String> kafkaConsumer, List<String> topics, Processor processor) {
        this.kafkaConsumer = kafkaConsumer;
        this.topics = topics;
        this.processor = processor;
    }

    public void start() {
        consumer =
            Flowable
                .<ConsumerRecords<String, String>>create(
                    emitter -> {
                        try {
                            kafkaConsumer.subscribe(topics);
                            while (!emitter.isCancelled()) {
                                emitter.onNext(kafkaConsumer.poll(Duration.ofMillis(Config.CONSUMER_POLL_TIMEOUT)));
                            }
                            emitter.onComplete();
                        } catch (Exception exception) {
                            emitter.onError(exception);
                        }
                    },
                    BackpressureStrategy.DROP
                )
                .onBackpressureDrop(this::monitorDrops)
                .filter(__ -> kafkaConsumer.assignment().size() > 0)
                .doOnNext(this::assignedToPartition)
                .filter(records -> records.count() > 0)
                .doOnNext(this::monitorConsumed)
                .concatMapSingle(processor::processBatch)
                .concatMapSingle(
                    x -> {
                        return Single.create(
                            emitter -> {
                                kafkaConsumer.commitAsync(
                                    (result, throwable) -> {
                                        if (throwable != null) {
                                            Monitor.commitFailed(throwable);
                                            emitter.onError(throwable);
                                            return;
                                        }
                                        emitter.onSuccess(result);
                                    }
                                );
                            }
                        );
                    }
                )
                .subscribeOn(Schedulers.io())
                .doFinally(
                    () -> {
                        kafkaConsumer.close();
                        ready = false;
                    }
                )
                .subscribe();
    }

    public void stop() {
        try {
            consumer.dispose();
            Thread.sleep(3 * Config.CONSUMER_POLL_TIMEOUT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean ready() {
        return ready;
    }

    private void assignedToPartition(ConsumerRecords<String, String> records) {
        if (!ready) {
            ready = true;
            Monitor.assignedToPartition();
        }
    }

    private void monitorConsumed(ConsumerRecords<String, String> records) {
        Monitor.consumed(records);
    }

    private void monitorDrops(ConsumerRecords<String, String> records) {
        Monitor.monitorDroppedRecords(records.count());
    }
}
