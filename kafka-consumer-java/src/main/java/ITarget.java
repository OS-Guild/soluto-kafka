import java.util.concurrent.CompletableFuture;
import java.util.Iterator;
import org.apache.kafka.common.header.Header;
import reactor.kafka.receiver.ReceiverRecord;

interface ITarget {
    CompletableFuture<TargetResponse> call(ReceiverRecord<String, String> record);

    default String getOriginalTopic(ReceiverRecord<String, String> record) {
        Iterator<Header> headers = record.headers().headers(Config.ORIGINAL_TOPIC).iterator();
        if (headers.hasNext()) {
            return String.valueOf(headers.next().value());
        }
        return record.topic();
    }
}
