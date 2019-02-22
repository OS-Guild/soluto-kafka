import java.util.Date;

import com.mashape.unirest.http.Unirest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ConsumerRecordRunnable implements Runnable {

    private final Config config;
    private final WriteMetric writeMetric;
    private KafkaProducer<String, String> producer;

    private final ConsumerRecord<String, String> consumerRecord;

    ConsumerRecordRunnable(
        Config config,
        WriteMetric writeMetric,
        KafkaProducer<String, String> producer,
        ConsumerRecord<String, String> consumerRecord){
            this.config = config;
            this.writeMetric = writeMetric;
            this.producer = producer;
            this.consumerRecord = consumerRecord;
    }

    public void run() {
        try {
            long executionStart = new Date().getTime();
            
            Unirest
                .post(config.TARGET_ENDPOINT)
                .header("Content-Type", "application/json")
                .body(consumerRecord.value().toString())
                .asString();

            writeMetric.process(executionStart);
            
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