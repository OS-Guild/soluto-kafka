public class ProducerRequest {
    String topic;
    String key;
    String message;

    ProducerRequest(String topic, String key, String message) {
        this.topic = topic;
        this.key = key;
        this.message = message;
    }
}
