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

    public boolean produce(ProducerRequest producerRequest) {
        var executionStart = (new Date()).getTime();
        kafkaProducer.send(
            new ProducerRecord<>(
                producerRequest.topic,
                null,
                executionStart,
                producerRequest.key,
                producerRequest.value,
                producerRequest.headers
            ),
            (metadata, err) -> {
                if (err != null) {
                    ready = false;
                    Monitor.produceError(err);
                    return;
                }
                ready = true;
                Monitor.produceSuccess(producerRequest, executionStart);
            }
        );
        return true;
    }

    public void close() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private void checkReadiness() {
        if (Config.READINESS_TOPIC == null) {
            ready = true;
            return;
        }
        kafkaProducer.send(
            new ProducerRecord<>(Config.READINESS_TOPIC, "ready"),
            (metadata, err) -> {
                if (err != null) {
                    Monitor.produceError(err);
                    ready = false;
                    return;
                }
                ready = true;
                Monitor.ready();
            }
        );
    }
}
