package kafka;

import configuration.Config;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import target.TargetFactory;
import target.TargetRetryPolicy;

public class ConsumerFactory {

    public static Consumer create(KafkaReceiver<String, String> kafkaReceiver) {
        return new Consumer(
            Flux.defer(kafkaReceiver::receive),
            TargetFactory.create(
                new TargetRetryPolicy(
                    new Producer(KafkaSender.<String, String>create(SenderOptions.create(KafkaOptions.producer()))),
                    Config.RETRY_TOPIC,
                    Config.DEAD_LETTER_TOPIC
                )
            ),
            Config.PROCESSING_DELAY
        );
    }
}
