import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class FlowableKafkaOnSubscribe implements FlowableOnSubscribe<ConsumerRecords<String, String>> {
    KafkaConsumer<String, String> consumer;

    FlowableKafkaOnSubscribe(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void subscribe(FlowableEmitter<ConsumerRecords<String, String>> emitter) throws Exception {
        while (true) {
            emitter.onNext(consumeRecordsFromKafka());
        }
    }

    private ConsumerRecords<String, String> consumeRecordsFromKafka() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Config.CONSUMER_POLL_TIMEOUT));
        if (records.count() > 0) System.out.println("Wowo " + records.count());
        return records;
    }
}
