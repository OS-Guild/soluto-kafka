import java.util.concurrent.CountDownLatch;

public class Main {
    static ConsumerRunner consumerRunner;
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

            consumerRunner =
                new ConsumerRunner(
                    KafkaClientFactory.createConsumer(),
                    Config.TOPICS,
                    ProcessorFactory.create(
                        new TargetRetryPolicy(
                            new ProduceSender(KafkaClientFactory.createProducer()),
                            Config.RETRY_TOPIC,
                            Config.DEAD_LETTER_TOPIC
                        ),
                        Config.PROCESSING_DELAY
                    )
                );

            monitoringServer = new MonitoringServer(consumerRunner, taretIsAlive);

            Runtime
                .getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> {
                            consumerRunner.stop();
                            monitoringServer.close();
                            Monitor.serviceShutdown();
                        }
                    )
                );

            monitoringServer.start();
            consumerRunner.start();
            Monitor.started();

            new CountDownLatch(1).await();
        } catch (Exception e) {
            Monitor.unexpectedError(e);
        } finally {
            monitoringServer.close();
            consumerRunner.stop();
            Monitor.serviceTerminated();
        }
    }
}
