import configuration.Config;
import kafka.Consumer;
import kafka.KafkaClientFactory;
import kafka.Producer;
import kafka.ReactiveKafkaClient;
import monitoring.Monitor;
import monitoring.MonitoringServer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import reactor.core.Disposable;
import target.TargetFactory;
import target.TargetIsAlive;
import target.TargetRetryPolicy;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

public class Main {
    static Disposable consumer;
    static MonitoringServer monitoringServer;
    static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {
        try {
            Config.init();
            Monitor.init();

            monitoringServer = new MonitoringServer(waitForTargetToBeAlive(), ).start();
            consumer = createConsumer(monitoringServer);
            onShutdown(consumer, monitoringServer);

            Monitor.started();
            latch.await();
        } catch (Exception e) {
            Monitor.initializationError(e);
        }
        Monitor.serviceTerminated();
    }

    private static TargetIsAlive waitForTargetToBeAlive() throws InterruptedException, IOException {
        var targetIsAlive = new TargetIsAlive();
        do {
            System.out.printf("waiting for target to be alive %s%n", targetIsAlive.getEndpoint());
            Thread.sleep(1000);
        } while (!targetIsAlive.check());
        System.out.println("target is alive");
        return targetIsAlive;
    }

    private static Disposable createConsumer(MonitoringServer monitoringServer) {
        return new Consumer(
                new ReactiveKafkaClient<>(
                        new KafkaClientFactory().createConsumer(),
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
                            }

                            @Override
                            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                                Monitor.revokedFromPartition(partitions);
                            }
                        }
                ),
                TargetFactory.create(
                        new TargetRetryPolicy(
                                new Producer(new KafkaClientFactory().createProducer()),
                                Config.RETRY_TOPIC,
                                Config.DEAD_LETTER_TOPIC
                        )
                )
        )
                .stream()
                .doOnError(
                        e -> {
                            Monitor.consumerError(e);
                        }
                )
                .subscribe(
                        __ -> {
                        },
                        exception -> {
                            monitoringServer.consumerDisposed();
                            Monitor.consumerError(exception);
                        },
                        () -> {
                            monitoringServer.consumerDisposed();
                            Monitor.consumerCompleted();
                        }
                );
    }

    private static void onShutdown(Disposable consumer, MonitoringServer monitoringServer) {
        Runtime
                .getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    Monitor.shuttingDown();
                                    consumer.dispose();
                                    monitoringServer.close();
                                    latch.countDown();
                                }
                        )
                );
    }
}
