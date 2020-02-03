import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;

class KafkaCreator {

    public KafkaCreator() {}

    private Properties getAuthProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.KAFKA_BROKER);

        if (!Config.AUTHENTICATED_KAFKA) {
            return props;
        }

        if (Config.SECURITY_PROTOCOL == "SSL") {
            props.put("security.protocol", "SSL");
            props.put("ssl.truststore.location", Config.TRUSTSTORE_LOCATION);
            props.put("ssl.truststore.password", Config.KAFKA_PASSWORD);
            props.put("ssl.keystore.type", "PKCS12");
            props.put("ssl.keystore.location", Config.KEYSTORE_LOCATION);
            props.put("ssl.keystore.password", Config.KAFKA_PASSWORD);
            props.put("ssl.key.password", Config.KAFKA_PASSWORD);
        } else if (Config.SECURITY_PROTOCOL == "SASL_SSL") {
            props.put("security.protocol", "SASL_SSL");
            props.put("ssl.endpoint.identification.algorithm", "https");
            props.put("sasl.mechanism", "PLAIN");
            props.put("sasl.jaas.config", Config.SASL_JAAS_CONFIG);
        }

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
