import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

class KafkaCreator {
    private Properties getAuthProperties() {
        var props = new Properties();
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

    public KafkaConsumer<String, String> createConsumer() {
        var props = getAuthProperties();
        props.put("group.id", Config.GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("max.poll.interval.ms", String.valueOf(Config.POLL_INTERVAL));
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
