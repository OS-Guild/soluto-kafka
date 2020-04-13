import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import java.net.ConnectException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ConsumerRunner implements IConsumerRunnerLifecycle {
    private final Processor processor;
    private final Partitioner partitioner;
    private KafkaConsumer<String, String> consumer;
    private List<String> topics;
    private boolean running;
    private boolean assignedToPartition;
    private int id;
    private Disposable disposableFlowable;

    ConsumerRunner(
        int id,
        KafkaConsumer<String, String> consumer,
        List<String> topics,
        long processingDelay,
        KafkaProducer<String, String> producer,
        String retryTopic,
        String deadLetterTopic
    ) {
        this.id = id;
        this.consumer = consumer;
        this.topics = topics;
        this.processor = new Processor(processingDelay, producer, retryTopic, deadLetterTopic);
        this.partitioner = new Partitioner();
    }

    public void start() {
        running = true;
        consumer.subscribe(topics);
        FlowableOnSubscribe<ConsumerRecords<String, String>> source =
            emitter -> {
                while (!emitter.isCancelled() && running) {
                    emitter.onNext(consumer.poll(Duration.ofMillis(Config.CONSUMER_POLL_TIMEOUT)));
                }
                consumer.unsubscribe();
                consumer.close();
                emitter.onComplete();
            };

        Flowable
            .create(source, BackpressureStrategy.DROP)
            .onBackpressureDrop(this::monitorDrops)
            .doOnNext(this::monitorPartitionStatus)
            .filter(records -> records.count() > 0)
            .doOnNext(this::monitorConsumed)
            .doOnError(this::handleErrorAndStop)
            //.delay(this.processingDelay, TimeUnit.MILLISECONDS)
            .map(records -> partitioner.partition(records))
            .flatMap(processor::process)
            .subscribeOn(Schedulers.io())
            .blockingSubscribe();
    }

    private void monitorConsumed(ConsumerRecords<String, String> records) {
        Monitor.consumed(records);
    }

    private void monitorDrops(ConsumerRecords<String, String> records) {
        Monitor.monitorDroppedRecords(id, records.count());
    }

    public void stop() {
        try {
            assignedToPartition = false;
            running = false;
            Thread.sleep(3 * Config.CONSUMER_POLL_TIMEOUT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleErrorAndStop(Throwable throwable) {
        throwable.printStackTrace();
        if (throwable.getCause() instanceof ConnectException) {
            Monitor.targetConnectionUnavailable();
        } else {
            Monitor.unexpectedError(throwable);
        }
        stop();
    }

    private void monitorPartitionStatus(ConsumerRecords<String, String> records) {
        if (!assignedToPartition && consumer.assignment().size() > 0) {
            assignedToPartition = true;
            Monitor.assignedToPartition(id);
        }
        if (!assignedToPartition) {
            Monitor.waitingForAssignment(id);
        }
    }

    private void commit(ConsumerRecord<String, String> record) {
        try {
            System.out.println("Commit");
            consumer.commitSync();
        } catch (CommitFailedException e) {
            Monitor.commitFailed(e);
        }
    }

    @Override
    public boolean assignedToPartition() {
        return assignedToPartition;
    }
}
