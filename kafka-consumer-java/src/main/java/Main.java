import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.List;

public class Main {
    static List<ConsumerLoopWrapper> consumerLoops = new ArrayList<>();
    static CountDownLatch countDownLatch;

    public static void main(String[] args) {
        try {
            Config.init();
            Monitor.init();
            var kafkaCreator = new KafkaCreator();
            countDownLatch = new CountDownLatch(Config.CONSUMER_THREADS);

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

            var monitoringServer = new MonitoringServer(consumerLoops);
            monitoringServer.start();

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

            Monitor.started();
            countDownLatch.await();
            monitoringServer.close();
            Monitor.serviceTerminated();
        } catch (Exception e) {
            Monitor.unexpectedError(e);
        }
    }
}
