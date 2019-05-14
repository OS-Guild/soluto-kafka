import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Main {
    static List<ConsumerLoopWrapper> consumerLoops = new ArrayList<>();
    static CountDownLatch countDownLatch = new CountDownLatch(Config.CONSUMER_THREADS);

    public static void main(String[] args) {
        try {
            Config.init();
            Monitor.init();
            var kafkaCreator = new KafkaCreator();
    
            var deadLetterProducer = kafkaCreator.createProducer();
            for (var i = 0; i < Config.CONSUMER_THREADS; i++) {
                var consumer = kafkaCreator.createConsumer();
                var consumerLoop = new ConsumerLoopWrapper(new ConsumerLoop(i, consumer, deadLetterProducer), countDownLatch);
                new Thread(consumerLoop).start();
                consumerLoops.add(consumerLoop);
            }
    
            var isAliveServer = new IsAliveServer(consumerLoops);
            isAliveServer.start();
    
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                consumerLoops.forEach(consumerLoop -> consumerLoop.stop());
                isAliveServer.close();
                Monitor.serviceShutdown();
            }));
    
            Monitor.serviceStarted();
            countDownLatch.await();
            isAliveServer.close();
            Monitor.serviceTerminated();
        }
        catch (Exception e) {
            Monitor.unexpectedError(e);
        }
    }
}
