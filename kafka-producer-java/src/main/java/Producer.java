import java.util.Date;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    Config config;
    Monitor monitor;
    KafkaProducer<String, String> kafkaProducer;
    boolean ready = false;

    Producer(Config config, Monitor monitor) {
        this.config = config;
        this.monitor = monitor;
    }

    public Producer start() {
        kafkaProducer = new KafkaCreator().createProducer();
        checkReadiness();
        return this;
    }

    public boolean ready() {
        checkReadiness();
        return ready;
    }

    public boolean produce(ProducerMessage message) {
        var executionStart = (new Date()).getTime();
        kafkaProducer.send(new ProducerRecord<>(Config.TOPIC, null, executionStart, message.key, message.value), (metadata, err) -> {
            if (err != null) {
                ready = false;
                Monitor.produceFail(err);
                return;
            }
            ready = true;
            Monitor.produceLatency(executionStart);
        });
        return true;
    }

    public void close() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private void checkReadiness() {
        kafkaProducer.send(new ProducerRecord<>(Config.READINESS_TOPIC, "ready"), (metadata, err) -> {
            if (err != null) {
                ready = false;
                return;
            }
            ready = true;
            Monitor.ready();
        });
    }
}