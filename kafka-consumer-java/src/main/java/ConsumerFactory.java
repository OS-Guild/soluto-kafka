import reactor.adapter.rxjava.RxJava2Adapter;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

public class ConsumerFactory {

    public static Consumer create(MonitoringServer monitoringServer) {
        return new Consumer(
            RxJava2Adapter.fluxToFlowable(
                KafkaReceiver
                    .create(
                        ReceiverOptions
                            .<String, String>create(KafkaOptions.consumer())
                            .subscription(Config.TOPICS)
                            .addAssignListener(
                                partitions -> {
                                    Monitor.assignedToPartition(partitions);
                                    monitoringServer.ready(partitions.size() > 0);
                                }
                            )
                            .addRevokeListener(
                                partitions -> {
                                    Monitor.revokedFromPartition(partitions);
                                    monitoringServer.ready(partitions.size() > 0);
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
    }
}
