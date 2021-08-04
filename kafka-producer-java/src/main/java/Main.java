public class Main {
    static Config config;
    static Monitor monitor;
    static AbstractProducer producer;
    static Server server;

    public static void main(String[] args) throws Exception {
        Config.init();
        System.out.println("config init");
        Monitor.init();
        System.out.println("monitor init");
        producer = createProducer(config);
        producer.initializeProducer();
        System.out.println("producer started");
        server = new Server(config, monitor, producer).start();
        System.out.println("server started");
        Runtime.getRuntime().addShutdownHook(new Thread(Main::close));
        Monitor.started();
    }

    private static AbstractProducer createProducer(Config config) {
        if (Config.BLOCKING) {
            return new SyncProducer(config, monitor);
        } else {
            return new AsyncProducer(config, monitor);
        }
    }

    private static void close() {
        producer.close();
        server.close();
        Monitor.serviceShutdown();
    }
}
