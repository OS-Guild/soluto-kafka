import java.util.concurrent.CountDownLatch;
import reactor.adapter.rxjava.RxJava2Adapter;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

public class Main {
    static Consumer consumer;
    static MonitoringServer monitoringServer;

    public static void main(String[] args) {
        try {
            Config.init();
            Monitor.init();

            var taretIsAlive = new TargetIsAlive();
            do {
                System.out.println("waiting for target to be alive");
                Thread.sleep(1000);
            } while (!taretIsAlive.check());
            System.out.println("target is alive");

            consumer =
                new Consumer(
                    RxJava2Adapter.fluxToFlowable(
                        KafkaReceiver
                            .create(
                                ReceiverOptions
                                    .<String, String>create(KafkaClientFactory.createConsumer())
                                    .subscription(Config.TOPICS)
                                    .addAssignListener(partitions -> Monitor.assignedToPartition())
                            )
                            .receive()
                    ),
                    TargetFactory.create(
                        new TargetRetryPolicy(
                            new ProduceSender(KafkaClientFactory.createProducer()),
                            Config.RETRY_TOPIC,
                            Config.DEAD_LETTER_TOPIC
                        )
                    ),
                    Config.PROCESSING_DELAY
                );

            monitoringServer = new MonitoringServer(taretIsAlive);

            Runtime
                .getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> {
                            consumer.stop();
                            monitoringServer.close();
                            Monitor.serviceShutdown();
                        }
                    )
                );

            monitoringServer.start();
            consumer.start();
            Monitor.started();

            new CountDownLatch(1).await();
        } catch (Exception e) {
            Monitor.unexpectedError(e);
        } finally {
            monitoringServer.close();
            consumer.stop();
            Monitor.serviceTerminated();
        }
    }
}
