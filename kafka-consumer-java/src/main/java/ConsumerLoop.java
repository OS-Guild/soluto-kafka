import java.net.ConnectException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ConsumerLoop implements Runnable, IConsumerLoopLifecycle {
    private final Processor processor;
    private final Partitioner partitioner;
    private KafkaConsumer<String, String> consumer;
    private List<String> topics;
    private boolean running;
    private boolean assignedToPartition;
    private int id;

    ConsumerLoop(
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

    @Override
    public void run() {
        running = true;
        consumer.subscribe(topics);
        try {
            while (running) {
                var consumed = consumer.poll(Duration.ofMillis(Config.CONSUMER_POLL_TIMEOUT));
                if (!assignedToPartition && consumer.assignment().size() > 0) {
                    assignedToPartition = true;
                    Monitor.assignedToPartition(id);
                }
                if (!assignedToPartition) {
                    Monitor.waitingForAssignment(id);
                }
                if (consumed.count() == 0) {
                    continue;
                }
                Monitor.consumed(consumed);

                var consumedPartitioned = partitioner.partition(consumed);

                var executionStart = new Date().getTime();
                processor.process(consumedPartitioned);
                Monitor.processBatchCompleted(executionStart);

                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    Monitor.commitFailed(e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (e.getCause() instanceof ConnectException) {
                Monitor.targetConnectionUnavailable();
            } else {
                Monitor.unexpectedError(e);
            }
        } finally {
            assignedToPartition = false;
            consumer.unsubscribe();
            consumer.close();
        }
    }

    public void stop() {
        running = false;
    }

    @Override
    public boolean assignedToPartition() {
        return assignedToPartition;
    }
}
