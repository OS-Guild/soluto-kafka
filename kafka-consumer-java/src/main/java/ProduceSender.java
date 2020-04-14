import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public class ProduceSender {
    private Producer<String, String> producer;

    public ProduceSender(Producer<String, String> producer) {
        this.producer = producer;
    }

    public void produce(String topicPrefix, String topic, ConsumerRecord<String, String> record) {
        Iterator<Header> headers = record.headers().headers(Config.ORIGINAL_TOPIC).iterator();
        Headers headersToSend;
        if (headers.hasNext()) {
            headersToSend = record.headers();
        } else {
            headersToSend = new RecordHeaders();
            headersToSend.add(Config.ORIGINAL_TOPIC, record.topic().getBytes());
        }
        producer.send(
            new ProducerRecord(topic, null, record.key(), record.value(), headersToSend),
            (metadata, err) -> {
                if (err != null) {
                    Monitor.produceError(topicPrefix, record, err);
                    return;
                }
            }
        );
    }
}
