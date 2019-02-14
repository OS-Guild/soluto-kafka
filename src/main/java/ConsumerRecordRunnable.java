import com.mashape.unirest.http.Unirest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ConsumerRecordRunnable implements Runnable {

    private final ConsumerRecord<String, String> consumerRecord;
    private final Config config;
    private KafkaProducer<String, String> producer;

    ConsumerRecordRunnable(ConsumerRecord<String, String> consumerRecord, Config config,
            KafkaProducer<String, String> producer) {
        this.consumerRecord = consumerRecord;
        this.config = config;
        this.producer = producer;
    }

    public void run() {
        try {
            Unirest.post(config.TARGET_ENDPOINT).header("Content-Type", "application/json")
                    .body(consumerRecord.value().toString()).asString();
        } catch (Exception e) {
            e.printStackTrace();
            producer.send(new ProducerRecord<>(config.DEAD_LETTER_TOPIC, consumerRecord.key().toString(),
                    consumerRecord.value().toString()), (metadata, e1) -> {
                        if (e1 != null) {
                            e1.printStackTrace();
                            return;
                        }
                        System.out.println("debug: sent message with key: " + consumerRecord.key() + "to dead letter");
                    });
        }
    }
}