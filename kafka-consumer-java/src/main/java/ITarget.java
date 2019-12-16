import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;

interface ITarget {
    CompletableFuture<TargetResponse> call(ConsumerRecord<String, String> record);
}
