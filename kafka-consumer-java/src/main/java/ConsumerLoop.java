import java.net.ConnectException;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ConsumerLoop implements Runnable, IConsumerLoopLifecycle {
    private final Processor processor;
    private final Partitioner partitioner;
    private KafkaConsumer<String, String> consumer;
    private boolean running;
    private boolean ready;
    private int id;

    ConsumerLoop(int id, KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer) {
        this.id = id;
        this.consumer = consumer;
        this.processor = new Processor(producer);
        this.partitioner = new Partitioner();
    }

    @Override
    public void run() {
        running = true;
        consumer.subscribe(Collections.singletonList(Config.TOPIC));
        try {
            while (running) {
                var consumed = consumer.poll(Duration.ofMillis(Config.CONSUMER_POLL_TIMEOUT));
                if (!ready && consumer.assignment().size() > 0) {
                    ready = true;
                    Monitor.ready(id);
                }
                if (consumed.count() == 0) continue;
                Monitor.consumed(consumed);

                var consumedPartitioned = partitioner.partition(consumed);
                Monitor.consumedPartitioned(consumedPartitioned);

                var executionStart = new Date().getTime();
                processor.process(consumedPartitioned);
                Monitor.processCompleted(executionStart);

                try {
                    consumer.commitSync();
                } catch (CommitFailedException ignored) {
                    Monitor.commitFailed();
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
            ready = false;
            consumer.unsubscribe();
            consumer.close();
        }
    }

    public void stop() {
        running = false;
    }

    @Override
    public boolean ready() {
        return ready;
    }
}
