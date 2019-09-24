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
        kafkaProducer = new KafkaCreator(config).createProducer();

        return this;
    }

    public boolean ready() {
        System.out.println("config.READINESS_TOPIC is " + config.READINESS_TOPIC);

        if (config.READINESS_TOPIC != null) {
            kafkaProducer.send(new ProducerRecord<>(config.READINESS_TOPIC, "ready"), (metadata, err) -> {
                if (err != null) {
                    ready = false;
                    return;
                }
                System.out.println("producer is ready");
                ready = true;
            });
        } else {
            System.out.println("producer is ready");
            ready = true;
        }
        return ready;
    }

    public boolean produce(ProducerMessage message) {
        var executionStart = (new Date()).getTime();
        kafkaProducer.send(new ProducerRecord<>(config.TOPIC, null, executionStart, message.key, message.value), (metadata, err) -> {
            if (err != null) {
                ready = false;
                monitor.produceFail(err);
                return;
            }
            ready = true;
            monitor.produceLatency(executionStart);
        });
        return true;
    }

    public void close() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}