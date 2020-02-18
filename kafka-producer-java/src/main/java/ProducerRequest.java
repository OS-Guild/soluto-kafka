public class ProducerRequest {
    String topic;
    String key;
    String value;

    ProducerRequest(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }
}
