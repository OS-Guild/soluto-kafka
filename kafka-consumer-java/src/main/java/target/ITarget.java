package target;

import configuration.Config;
import java.util.concurrent.CompletableFuture;
import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public interface ITarget {
    CompletableFuture<TargetResponse> call(ConsumerRecord<String, String> record);

    default String getOriginalTopic(ConsumerRecord<String, String> record) {
        Iterator<Header> headers = record.headers().headers(Config.ORIGINAL_TOPIC).iterator();
        if (headers.hasNext()) {
            return new String(headers.next().value());
        }
        return record.topic();
    }

    default String getMessageHeaders(ConsumerRecord<String, String> record) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (record.headers() != null) {
            Iterator<Header> headers = record.headers().iterator();
            boolean first = true;
            while (headers.hasNext()) {
                if (!first) {
                    stringBuilder.append(';');
                    first = false;
                }
                Header header = headers.next();
                stringBuilder.append(header.key());
                stringBuilder.append(':');
                stringBuilder.append(new String(header.value()));
            }
        }
        return stringBuilder.toString();
    }
}
