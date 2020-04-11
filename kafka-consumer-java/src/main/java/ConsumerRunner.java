import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import java.net.ConnectException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ConsumerRunner implements IConsumerLoopLifecycle {
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
        System.out.println("Creating consumer " + id);
        this.id = id;
        this.consumer = consumer;
        this.topics = topics;
        this.processor = new Processor(processingDelay, producer, retryTopic, deadLetterTopic);
        this.partitioner = new Partitioner();
    }

    public void start() {
        running = true;
        System.out.println("Topics: " + topics);
        consumer.subscribe(topics);
        disposableFlowable =
            Flowable
                .create(new FlowableKafkaOnSubscribe(consumer), BackpressureStrategy.BUFFER)
                .subscribeOn(Schedulers.io())
                .doOnError(this::handleErrorAndStop)
                .subscribe(this::processRecords);
    }

    public void stop() {
        try {
            if (disposableFlowable != null) {
                disposableFlowable.dispose();
            }
            assignedToPartition = false;
            consumer.unsubscribe();
            consumer.close();
            running = false;
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

    private void processRecords(ConsumerRecords<String, String> records) throws InterruptedException {
        if (!assignedToPartition && consumer.assignment().size() > 0) {
            assignedToPartition = true;
            Monitor.assignedToPartition(id);
        }
        if (!assignedToPartition) {
            Monitor.waitingForAssignment(id);
        }
        if (records.count() == 0) {
            return;
        }
        System.out.println("I'm in");

        Monitor.consumed(records);

        var consumedPartitioned = partitioner.partition(records);

        var executionStart = new Date().getTime();
        processor.process(consumedPartitioned);
        Monitor.processBatchCompleted(executionStart);

        try {
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
