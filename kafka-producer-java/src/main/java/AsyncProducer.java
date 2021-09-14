import java.util.Date;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsyncProducer extends AbstractProducer {

    AsyncProducer(Config config, Monitor monitor) {
        super(config, monitor);
    }

    @Override
    public void produce(ProducerRequest producerRequest) {
        var executionStart = (new Date()).getTime();
        kafkaProducer.send(
            createRecord(producerRequest, executionStart),
            (RecordMetadata metadata, Exception err) -> {
                if (err != null) {
                    ready = false;
                    Monitor.produceError(err);
                    return;
                }
                ready = true;
                Monitor.produceSuccess(producerRequest, executionStart);
            }
        );
    }
}
