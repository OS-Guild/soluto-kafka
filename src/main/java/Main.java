import java.util.ArrayList;
import java.util.List;

public class Main {
    static List<ConsumerLoop> consumerLoops = new ArrayList<>();
    public static void main(String[] args) throws Exception {
        Config.init();
        Monitor.init();
        var deadLetterProducer = new KafkaCreator().createProducer();

        var consumer = new KafkaCreator().createConsumer();
        var consumerLoop1 = new ConsumerLoop(1, consumer, deadLetterProducer);
        new Thread(consumerLoop1).start();
        consumerLoops.add(consumerLoop1);

        var server = new IsAliveServer(consumerLoops).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumerLoops.forEach(consumerLoop -> consumerLoop.stop());
            server.close();
        }));

        Monitor.serviceStarted();
    }
}
