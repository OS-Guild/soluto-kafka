import com.mashape.unirest.http.Unirest;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerThreadHandler implements Runnable {

    private ConsumerRecord consumerRecord;

    ConsumerThreadHandler(ConsumerRecord consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    public void run() {
        System.out.printf("offset = %d, partition = %d \n", consumerRecord.offset(), consumerRecord.partition());
        try {
            Unirest.post("http://localhost:4000/post")
                    .header("Content-Type", "application/json")
                    .body(consumerRecord.value().toString())
                    .asString();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}