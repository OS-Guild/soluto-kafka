import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractProducer {
    Config config;
    Monitor monitor;
    KafkaProducer<String, String> kafkaProducer;
    boolean ready = false;

    AbstractProducer(Config config, Monitor monitor) {
        this.config = config;
        this.monitor = monitor;
    }

    public void initializeProducer() {
        kafkaProducer = new KafkaCreator().createProducer();
        checkReadiness();
    }

    public boolean ready() {
        checkReadiness();
        return ready;
    }

    public abstract boolean produce(ProducerRequest producerRequest);

    @NotNull
    protected ProducerRecord<String, String> createRecord(ProducerRequest producerRequest, long executionStart) {
        return new ProducerRecord<>(
            producerRequest.topic,
            null,
            executionStart,
            producerRequest.key,
            producerRequest.value,
            producerRequest.headers
        );
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
