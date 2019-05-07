public class ProducerMessage {
    String key;
    String value;

    ProducerMessage(String key, String message) {
        this.key = key;
        this.value = message;
    }
}