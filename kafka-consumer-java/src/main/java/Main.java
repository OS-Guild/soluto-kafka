import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.List;

public class Main {
    static List<ConsumerRunner> consumerRunners = new ArrayList<>();
    static CountDownLatch countDownLatch;
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

            var kafkaCreator = new KafkaCreator();
            var producer = kafkaCreator.createProducer();

            if (Config.TOPICS != null) {
                var consumerRunner = new ConsumerRunner(
                    kafkaCreator.createConsumer(),
                    Config.TOPICS,
                    ProcessorFactory.create(
                        new TargetRetryPolicy(
                            new ProduceSender(producer),
                            Config.RETRY_TOPIC,
                            Config.DEAD_LETTER_TOPIC
                        ),
                        Config.PROCESSING_DELAY
                    )
                );
                consumerRunner.start();
                consumerRunners.add(consumerRunner);
            }

            if (Config.RETRY_TOPIC != null) {
                var retryConsumerRunner = new ConsumerRunner(
                    kafkaCreator.createConsumer(),
                    Collections.singletonList(Config.RETRY_TOPIC),
                    ProcessorFactory.create(
                        new TargetRetryPolicy(new ProduceSender(producer), null, Config.DEAD_LETTER_TOPIC),
                        Config.RETRY_PROCESSING_DELAY
                    )
                );
                retryConsumerRunner.start();
                consumerRunners.add(retryConsumerRunner);
            }

            Runtime
                .getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> {
                            consumerRunners.forEach(runner -> runner.stop());
                            monitoringServer.close();
                            Monitor.serviceShutdown();
                        }
                    )
                );

            monitoringServer = new MonitoringServer(consumerRunners, taretIsAlive);
            monitoringServer.start();
            Monitor.started();

            countDownLatch = new CountDownLatch(consumerRunners.size());
            countDownLatch.await();
        } catch (Exception e) {
            Monitor.unexpectedError(e);
        } finally {
            if (monitoringServer != null) {
                monitoringServer.close();
            }
            consumerRunners.forEach(consumerRunner -> consumerRunner.stop());
            Monitor.serviceTerminated();
            System.exit(0);
        }
    }
}
