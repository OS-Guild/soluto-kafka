import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.io.IOException;

class Processor {
    ITarget target;
    
    Processor(KafkaProducer<String, String> kafkaProducer) {
        var targetRetryPolicy = new TargetRetryPolicy(new Producer(kafkaProducer));
        target = Config.SENDING_PROTOCOL.equals("grpc") ? new GrpcTarget(targetRetryPolicy) : new HttpTarget(targetRetryPolicy);
    }

    void process(Iterable<Iterable<ConsumerRecord<String, String>>> partitions) throws IOException, InterruptedException {
        Thread.sleep(Config.PROCESSING_DELAY);
        Flowable.fromIterable(partitions)
                .flatMap(this::processPartition, Config.CONCURRENCY)
                .subscribeOn(Schedulers.io())
                .blockingSubscribe();
    }

    private Flowable processPartition(Iterable<ConsumerRecord<String, String>> partition) {
        return Flowable.fromIterable(partition)
            .doOnNext(Monitor::messageLatency)
            .flatMap(record -> Flowable.fromFuture(target.call(record)), Config.CONCURRENCY_PER_PARTITION)
            .doOnNext(x -> {
                if (x.callLatency.isPresent()) {
                    Monitor.callTargetLatency(x.callLatency.getAsLong());
                }
                if (x.resultLatency.isPresent()) {
                    Monitor.resultTargetLatency(x.resultLatency.getAsLong());
                }                    
            })
            .flatMap(x -> x.type == TargetResponseType.Error ? Flowable.error(x.exception.getCause()) : Flowable.empty());
    }


}
