public class Main {
    static Config config;
    static Monitor monitor;
    static Producer producer;
    static Server server;

    public static void main(String[] args) throws Exception {
        init();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
        monitor.serviceStarted();

        Runtime runtime = Runtime.getRuntime();
        int processors = runtime.availableProcessors();
        long maxMemory = runtime.maxMemory();

        System.out.format("Number of processors: %d\n", processors);
        System.out.format("Max memory: %d bytes\n", maxMemory);
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
