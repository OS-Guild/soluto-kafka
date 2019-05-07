import java.util.Date;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    Config config;
    Monitor monitor;
    KafkaProducer<String, String> kafkaProducer;
    boolean didLastProduceFailed = false;
    
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

    public boolean produce(ProducerMessage message) {
        var executionStart = (new Date()).getTime();
        kafkaProducer.send(new ProducerRecord<>(config.TOPIC, null, executionStart, message.key, message.value), (metadata, err) -> {
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
        kafkaProducer.close();
    }
}