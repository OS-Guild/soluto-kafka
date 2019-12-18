import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    private KafkaProducer<String, String> producer;

    public Producer(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public void produce(String topicPrefix, String topic, ConsumerRecord<String, String> record) {
        producer.send(
            new ProducerRecord<>(topic, record.key(), record.value()),
            (metadata, err) -> {
                if (err != null) {
                    Monitor.produceError(topicPrefix, record, err);
                    return;
                }

                Monitor.topicProduced(topicPrefix, record);
            }
        );
    }
}
