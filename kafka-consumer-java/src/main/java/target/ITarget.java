package target;

import configuration.Config;
import java.util.concurrent.CompletableFuture;
import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.json.JSONObject;

public interface ITarget {
    CompletableFuture<TargetResponse> call(ConsumerRecord<String, String> record);

    default String getOriginalTopic(ConsumerRecord<String, String> record) {
        Iterator<Header> headers = record.headers().headers(Config.ORIGINAL_TOPIC).iterator();
        if (headers.hasNext()) {
            var header = headers.next();
            return header.value() == null ? null : new String(header.value());
        }
        return record.topic();
    }

    default String getRecordHeaders(ConsumerRecord<String, String> record) {
        JSONObject headersJson = new JSONObject();
        if (record.headers() != null) {
            Iterator<Header> headers = record.headers().iterator();
            while (headers.hasNext()) {
                Header header = headers.next();
                headersJson.put(header.key(), header.value() == null ? JSONObject.NULL : new String(header.value()));
            }
        }
        return headersJson.toString();
    }
}
