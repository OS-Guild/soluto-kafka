import java.util.concurrent.ExecutionException;
import java.util.Date;

public class SyncProducer extends AbstractProducer {

    SyncProducer(Config config, Monitor monitor) {
        super(config, monitor);
    }

    @Override
    public void produce(ProducerRequest producerRequest) throws ExecutionException, InterruptedException {
        var executionStart = (new Date()).getTime();
        try {
            kafkaProducer.send(createRecord(producerRequest, executionStart)).get();
            Monitor.produceSuccess(producerRequest, executionStart);
            ready = true;
        } catch (InterruptedException | ExecutionException e) {
            Monitor.produceError(e);
            throw e;
        }
    }
}
