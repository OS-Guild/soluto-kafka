import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

public class Producer {
    private KafkaSender<String, String> sender;

    public Producer(KafkaSender<String, String> sender) {
        this.sender = sender;
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

        sender
            .send(
                Flux.just(
                    SenderRecord.create(
                        new ProducerRecord<String, String>(topic, null, record.key(), record.value(), headersToSend),
                        record.key()
                    )
                )
            )
            .doOnError(error -> Monitor.produceError(topicPrefix, record, error))
            .subscribe(__ -> {}, __ -> {}, () -> {});
    }
}
