import java.util.Date;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    Config config;
    Monitor monitor;
    KafkaProducer<String, String> kafkaProducer;
    boolean didLastProduceFailed = false;
    boolean ready = false;

    Producer(Config config, Monitor monitor) {
        this.config = config;
        this.monitor = monitor;
    }

    public Producer start() {
        kafkaProducer = new KafkaCreator(config).createProducer();

        return this;
    }

    public boolean didLastProduceFailed() {
        return didLastProduceFailed;
    }

    public boolean ready() {
        if (config.READINESS_TOPIC != null) {
            kafkaProducer.send(new ProducerRecord<>(config.READINESS_TOPIC, "ready"), (metadata, err) -> {
                if (err != null) {
                    ready = true;
                    return;
                }
                ready = false;
            });
        } else {
            ready = true;
        }
        return ready;
    }

    public boolean produce(ProducerMessage message) {
        var executionStart = (new Date()).getTime();
        kafkaProducer.send(new ProducerRecord<>(config.TOPIC, null, executionStart, message.key, message.value),
                (metadata, err) -> {
                    if (err != null) {
                        didLastProduceFailed = true;
                        monitor.produceFail(err);
                        return;
                    }
                    didLastProduceFailed = false;
                    monitor.produceLatency(executionStart);
                });
        return true;
    }

    public void close() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}