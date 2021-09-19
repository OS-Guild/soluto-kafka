import java.util.concurrent.ExecutionException;
import java.util.Date;

public class SyncProducer extends AbstractProducer {

    SyncProducer(Config config, Monitor monitor) {
        super(config, monitor);
    }

    @Override
    public void produce(ProducerRequest producerRequest) throws Exception {
        var executionStart = (new Date()).getTime();
        try {
            System.out.println("sending!");
            kafkaProducer.send(createRecord(producerRequest, executionStart)).get();
            System.out.println("success!");
            Monitor.produceSuccess(producerRequest, executionStart);
            ready = true;
            throw new Exception("yoav fail");
        } catch (Exception e) {
            System.out.println("before monitor fail!");
            Monitor.produceError(e);

            //            ready = false;
            throw e;
        }
    }
}
