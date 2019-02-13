import com.mashape.unirest.http.Unirest;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerRecordRunnable implements Runnable {

    private final ConsumerRecord consumerRecord;
    private final Config config;

    ConsumerRecordRunnable(ConsumerRecord consumerRecord, Config config) {
        this.consumerRecord = consumerRecord;
        this.config = config;
    }

    public void run() {
        try {
            Unirest.post(config.TARGET_ENDPOINT)
                    .header("Content-Type", "application/json")
                    .body(consumerRecord.value().toString())
                    .asString();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}