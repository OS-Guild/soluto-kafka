import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

class KafkaCreator {
    private Config config;

    public KafkaCreator(Config config) {
        this.config = config;
    }

    private Properties getAuthProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.KAFKA_BROKER);

        if(config.SHOULD_SKIP_AUTHENTICATION) {
            return props;
        }

        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", config.TRUSTSTORE_LOCATION);
        props.put("ssl.truststore.password", config.KAFKA_PASSWORD);
        props.put("ssl.keystore.type", "PKCS12");
        props.put("ssl.keystore.location", config.KEYSTORE_LOCATION);
        props.put("ssl.keystore.password", config.KAFKA_PASSWORD);
        props.put("ssl.key.password", config.KAFKA_PASSWORD);

        return props;
    }

    public KafkaConsumer<String, String> createConsumer() {
        Properties props = getAuthProperties();
        props.put("group.id", config.GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("max.poll.records", String.valueOf(50));

        return new KafkaConsumer<>(props);
    }

    public KafkaProducer<String, String> createProducer() {
        Properties props = getAuthProperties();

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }
}
