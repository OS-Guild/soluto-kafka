import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

class KafkaCreator {

    private Properties getAuthProperties() {
        var props = new Properties();
        props.put("bootstrap.servers", Config.KAFKA_BROKER);

        if (!Config.AUTHENTICATED_KAFKA) {
            return props;
        }

        props.put("security.protocol", Config.SECURITY_PROTOCOL);

        if (Config.TRUSTSTORE_PASSWORD != null) {
            props.put("ssl.truststore.location", Config.TRUSTSTORE_LOCATION);
            props.put("ssl.truststore.password", Config.TRUSTSTORE_PASSWORD);
        }

        if (Config.SECURITY_PROTOCOL.equals("SSL")) {
            props.put("ssl.keystore.type", "PKCS12");
            props.put("ssl.keystore.location", Config.KEYSTORE_LOCATION);
            props.put("ssl.keystore.password", Config.KEYSTORE_PASSWORD);
            props.put("ssl.key.password", Config.KEY_PASSWORD);
        }

        if (Config.SECURITY_PROTOCOL.equals("SASL_SSL")) {
            props.put("sasl.mechanism", "PLAIN");
            props.put("ssl.endpoint.identification.algorithm", "https");
            props.put(
                "sasl.jaas.config",
                String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    Config.SASL_USERNAME,
                    Config.SASL_PASSWORD
                )
            );
        }

        return props;
    }

    public KafkaConsumer<String, String> createConsumer() {
        var props = getAuthProperties();
        props.put("group.id", Config.GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("max.poll.records", String.valueOf(Config.POLL_RECORDS));

        return new KafkaConsumer<>(props);
    }

    public KafkaProducer<String, String> createProducer() {
        var props = getAuthProperties();

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }
}
