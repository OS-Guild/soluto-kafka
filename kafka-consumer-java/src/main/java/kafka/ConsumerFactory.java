package kafka;

import configuration.Config;
import java.util.Collection;
import monitoring.Monitor;
import monitoring.MonitoringServer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import target.TargetFactory;
import target.TargetRetryPolicy;

public class ConsumerFactory {

    public static Consumer create(MonitoringServer monitoringServer) {
        return new Consumer(
            new ConsumerFlux<String, String>(
                new KafkaCreator(),
                Config.TOPICS,
                new ConsumerRebalanceListener() {

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        Monitor.assignedToPartition(partitions);
                        monitoringServer.consumerAssigned();
                    }

                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        Monitor.revokedFromPartition(partitions);
                        monitoringServer.consumerRevoked();
                    }
                }
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
