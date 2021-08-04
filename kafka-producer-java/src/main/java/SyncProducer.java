import java.util.concurrent.ExecutionException;
import java.util.Date;

public class SyncProducer extends AbstractProducer {

    SyncProducer(Config config, Monitor monitor) {
        super(config, monitor);
    }

    @Override
    public boolean produce(ProducerRequest producerRequest) {
        var executionStart = (new Date()).getTime();
        try {
            kafkaProducer.send(createRecord(producerRequest, executionStart)).get();
            ready = true;
            Monitor.produceSuccess(producerRequest, executionStart);
        } catch (InterruptedException | ExecutionException e) {
            ready = false;
            Monitor.produceError(e);
            return false;
        }
        return true;
    }
}
