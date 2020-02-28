import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.List;

public class Main {
    static List<ConsumerLoopWrapper> consumerLoops = new ArrayList<>();
    static CountDownLatch countDownLatch;
    static MonitoringServer monitoringServer;

    public static void main(String[] args) {
        try {
            Thread.sleep(20000);
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

            for (var i = 0; i < Config.CONSUMER_THREADS; i++) {
                var consumer = kafkaCreator.createConsumer();
                var consumerLoop = new ConsumerLoopWrapper(
                    new ConsumerLoop(
                        i,
                        consumer,
                        Config.TOPIC,
                        Config.PROCESSING_DELAY,
                        producer,
                        Config.RETRY_TOPIC,
                        Config.DEAD_LETTER_TOPIC
                    ),
                    countDownLatch
                );
                new Thread(consumerLoop).start();
                consumerLoops.add(consumerLoop);
            }

            if (Config.RETRY_TOPIC != null) {
                var retryConsumer = kafkaCreator.createConsumer();
                var retryConsumerLoop = new ConsumerLoopWrapper(
                    new ConsumerLoop(
                        0,
                        retryConsumer,
                        Config.RETRY_TOPIC,
                        Config.RETRY_PROCESSING_DELAY,
                        producer,
                        null,
                        Config.DEAD_LETTER_TOPIC
                    ),
                    countDownLatch
                );
                new Thread(retryConsumerLoop).start();
                consumerLoops.add(retryConsumerLoop);
            }

            Runtime
                .getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> {
                            consumerLoops.forEach(consumerLoop -> consumerLoop.stop());
                            monitoringServer.close();
                            Monitor.serviceShutdown();
                        }
                    )
                );

            monitoringServer = new MonitoringServer(consumerLoops, taretIsAlive);
            monitoringServer.start();
            Monitor.started();

            countDownLatch = new CountDownLatch(consumerLoops.size());
            countDownLatch.await();
        } catch (Exception e) {
            Monitor.unexpectedError(e);
            consumerLoops.forEach(consumerLoop -> consumerLoop.stop());
        } finally {
            if (monitoringServer != null) {
                monitoringServer.close();
            }
            Monitor.serviceTerminated();
            System.exit(0);
        }
    }
}
