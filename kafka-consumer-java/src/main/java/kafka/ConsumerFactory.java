package kafka;

import configuration.Config;
import io.reactivex.disposables.Disposable;
import monitoring.Monitor;
import reactor.adapter.rxjava.RxJava2Adapter;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import target.TargetFactory;
import target.TargetRetryPolicy;

public class ConsumerFactory {

    public static Disposable create() {
        var consumer = new Consumer(
            RxJava2Adapter.fluxToFlowable(
                KafkaReceiver
                    .create(
                        ReceiverOptions
                            .<String, String>create(KafkaOptions.consumer())
                            .subscription(Config.TOPICS)
                            .addAssignListener(
                                partitions -> {
                                    Monitor.assignedToPartition(partitions);
                                }
                            )
                            .addRevokeListener(
                                partitions -> {
                                    Monitor.revokedFromPartition(partitions);
                                }
                            )
                    )
                    .receive()
            ),
            TargetFactory.create(
                new TargetRetryPolicy(
                    new Producer(KafkaSender.<String, String>create(SenderOptions.create(KafkaOptions.producer()))),
                    Config.RETRY_TOPIC,
                    Config.DEAD_LETTER_TOPIC
                )
            ),
            Config.PROCESSING_DELAY
        );

        return consumer
            .stream()
            .subscribe(
                __ -> {},
                exception -> {
                    Monitor.unexpectedConsumerError(exception);
                }
            );
    }
}
