import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class MonitoringServer {
    List<? extends IConsumerRunner> consumerRunners;
    HttpServer server;
    TargetIsAlive targetIsAlive;

    public MonitoringServer(final List<? extends IConsumerRunner> consumerRunners, TargetIsAlive targetIsAlive) {
        this.consumerRunners = consumerRunners;
        this.targetIsAlive = targetIsAlive;
    }

    public void start() throws IOException {
        if (Config.MONITORING_SERVER_PORT == 0) {
            return;
        }

        server = HttpServer.create(new InetSocketAddress(Config.MONITORING_SERVER_PORT), 0);
        isAliveGetRoute(server);
        if (Config.USE_PROMETHEUS) {
            DefaultExports.initialize();
            new HTTPServer(server, CollectorRegistry.defaultRegistry, false);
        } else {
            server.start();
        }
    }

    public void close() {
        server.stop(0);
    }

    private void isAliveGetRoute(final HttpServer server) {
        final var httpContext = server.createContext("/isAlive");

        httpContext.setHandler(
            new HttpHandler() {

                @Override
                public void handle(final HttpExchange exchange) throws IOException {
                    if (!exchange.getRequestMethod().equals("GET")) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }

                    if (!targetAlive(exchange)) {
                        writeResponse(500, exchange);
                        return;
                    }

                    if (!consumerRunnersReady(consumerRunners)) {
                        writeResponse(500, exchange);
                        return;
                    }
                    writeResponse(200, exchange);
                }
            }
        );
    }

    private static boolean consumerRunnersReady(List<? extends IConsumerRunner> consumerRunners) {
        var response = consumerRunners.stream().map(x -> x.ready()).allMatch(y -> y.equals(true));
        if (!response) {
            Monitor.consumerNotAssignedToAtLeastOnePartition();
        }
        return response;
    }

    private boolean targetAlive(HttpExchange exchange) throws IOException {
        if (Config.TARGET_IS_ALIVE_HTTP_ENDPOINT != null) {
            try {
                return this.targetIsAlive.check();
            } catch (Exception e) {
                Monitor.unexpectedError(e);
                return false;
            }
        }
        return true;
    }

    private void writeResponse(int statusCode, HttpExchange exchange) throws IOException {
        final var os = exchange.getResponseBody();
        var responseText = (statusCode == 500 ? "false" : "true");
        exchange.sendResponseHeaders(statusCode, responseText.getBytes().length);
        os.write(responseText.getBytes());
        os.close();
    }
}
