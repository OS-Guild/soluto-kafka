public class Main {
    static Config config;
    static Monitor monitor;
    static Producer producer;
    static Server server;

    public static void main(String[] args) throws Exception {
        init();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
        monitor.serviceStarted();
    }

    private static void init() throws Exception {
        config = new Config();
        monitor = new Monitor(config);        
        producer = new Producer(config, monitor).start();
        server = new Server(config, monitor, producer).start();
    }

    private static void close() {
        producer.close();
        server.close();
        monitor.serviceShutdown();
    }
}
