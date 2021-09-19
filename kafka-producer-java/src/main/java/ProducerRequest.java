import java.util.Map;
import org.apache.kafka.common.header.Header;

public class ProducerRequest {
    String topic;
    String key;
    String value;
    Iterable<Header> headers;

    ProducerRequest(String topic, String key, String value, Iterable<Header> headers) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    @Override
    public String toString() {
        return (
            "ProducerRequest{" +
            "topic='" +
            topic +
            '\'' +
            ", key='" +
            key +
            '\'' +
            ", value='" +
            value +
            '\'' +
            ", headers=" +
            headers +
            '}'
        );
    }
}
