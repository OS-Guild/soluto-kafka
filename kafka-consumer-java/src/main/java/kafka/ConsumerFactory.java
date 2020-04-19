package kafka;

import configuration.Config;
import io.reactivex.disposables.Disposable;
import java.time.Duration;
import monitoring.Monitor;
import monitoring.MonitoringServer;
import reactor.adapter.rxjava.RxJava2Adapter;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import target.TargetFactory;
import target.TargetRetryPolicy;

public class ConsumerFactory {

    public static Disposable create(MonitoringServer monitoringServer) {
        var consumer = new Consumer(
            RxJava2Adapter.fluxToFlowable(
                KafkaReceiver
                    .create(
                        ReceiverOptions
                            .<String, String>create(KafkaOptions.consumer())
                            .subscription(Config.TOPICS)
                            .commitInterval(Duration.ZERO)
                            .addAssignListener(
                                partitions -> {
                                    Monitor.assignedToPartition(partitions);
                                    monitoringServer.consumerAssigned();
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
