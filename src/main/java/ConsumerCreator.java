import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

class ConsumerCreator {
    private ConsumerCreator() {
        // no instance
    }

    static KafkaConsumer<String, String> create(Config config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.KAFKA_BROKER);
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", config.TRUSTSTORE_LOCATION);
        props.put("ssl.truststore.password", config.KAFKA_PASSWORD);
        props.put("ssl.keystore.type", "PKCS12");
        props.put("ssl.keystore.location", config.KEYSTORE_LOCATION);
        props.put("ssl.keystore.password", config.KAFKA_PASSWORD);
        props.put("ssl.key.password", config.KAFKA_PASSWORD);
        props.put("group.id", config.GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("max.poll.interval.ms", String.valueOf(Integer.MAX_VALUE));
        props.put("max.poll.records", String.valueOf(config.POLL_RECORDS));

        return new KafkaConsumer<>(props);
    }
}
