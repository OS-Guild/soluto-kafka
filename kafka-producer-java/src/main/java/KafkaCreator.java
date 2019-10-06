import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

class KafkaCreator {
    public KafkaCreator() {
    }

    private Properties getAuthProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.KAFKA_BROKER);

        if(Config.SHOULD_SKIP_AUTHENTICATION) {
            return props;
        }

        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", Config.TRUSTSTORE_LOCATION);
        props.put("ssl.truststore.password", Config.KAFKA_PASSWORD);
        props.put("ssl.keystore.type", "PKCS12");
        props.put("ssl.keystore.location", Config.KEYSTORE_LOCATION);
        props.put("ssl.keystore.password", Config.KAFKA_PASSWORD);
        props.put("ssl.key.password", Config.KAFKA_PASSWORD);

        return props;
    }

    public KafkaProducer<String, String> createProducer() {
        Properties props = getAuthProperties();

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", Config.LINGER_TIME_MS);
        props.put("compression.type	", Config.COMPRESSION_TYPE);

        return new KafkaProducer<>(props);
    }
}
