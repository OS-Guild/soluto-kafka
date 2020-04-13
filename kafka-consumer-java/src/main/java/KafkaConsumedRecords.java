import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class KafkaConsumedRecords {

    public KafkaConsumedRecords(ConsumerRecords<String, String> records) {
        this.records = records;
    }

    public ConsumerRecords<String, String> getRecords() {
        return records;
    }

    public void setRecords(ConsumerRecords<String, String> records) {
        this.records = records;
    }

    public Iterable<Iterable<ConsumerRecord<String, String>>> getIterablePartitions() {
        return iterablePartitions;
    }

    public void setIterablePartitions(Iterable<Iterable<ConsumerRecord<String, String>>> iterablePartitions) {
        this.iterablePartitions = iterablePartitions;
    }

    ConsumerRecords<String, String> records;
    Iterable<Iterable<ConsumerRecord<String, String>>> iterablePartitions;
}
