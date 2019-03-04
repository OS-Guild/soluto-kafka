import java.util.ArrayList;
import java.util.List;

public class Main {
    static List<ConsumerLoop> consumerLoops = new ArrayList<>();
    public static void main(String[] args) throws Exception {
        Config.init();
        Monitor.init();
        var kafkaCreator = new KafkaCreator();

        var deadLetterProducer = kafkaCreator.createProducer();
        for (var i = 0; i < Config.CONSUMER_THREADS; i++) {
            var consumer = kafkaCreator.createConsumer();
            var consumerLoop = new ConsumerLoop(i, consumer, deadLetterProducer);
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
    }
}
