public class Main {
    static Config config;
    static Monitor monitor;
    static Producer producer;
    static Server server;

    public static void main(String[] args) throws Exception {
        Config.init();
        System.out.println("config init");
        Monitor.init();
        System.out.println("monitor init");
        producer = new Producer(config, monitor).start();
        System.out.println("producer started");
        server = new Server(config, monitor, producer).start();
        System.out.println("server started");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
        Monitor.started();
    }

    private static void close() {
        producer.close();
        server.close();
        Monitor.serviceShutdown();
    }
}
