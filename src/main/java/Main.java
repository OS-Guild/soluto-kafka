import java.util.ArrayList;
import java.util.List;

public class Main {
    static List<ConsumerLoop> consumerLoops = new ArrayList<>();
    public static void main(String[] args) throws Exception {
        Config.init();
        Monitor.init();
        var deadLetterProducer = new KafkaCreator().createProducer();

        for (var i = 0; i < Config.CONSUMER_THREADS; i++) {
            var consumer = new KafkaCreator().createConsumer();
            var consumerLoop1 = new ConsumerLoop(i, consumer, deadLetterProducer);
            new Thread(consumerLoop1).start();
            consumerLoops.add(consumerLoop1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumerLoops.forEach(consumerLoop -> consumerLoop.stop());
        }));

        Monitor.serviceStarted();
    }
}
