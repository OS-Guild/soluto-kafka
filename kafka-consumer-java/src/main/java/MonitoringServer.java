import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;

public class MonitoringServer {
    List<? extends IConsumerLoopLifecycle> consumerLoopLifecycles;
    HttpServer server;
    private static final HttpClient client = HttpClient.newHttpClient();

    public MonitoringServer(final List<? extends IConsumerLoopLifecycle> consumerLoopLifecycles) {
        this.consumerLoopLifecycles = consumerLoopLifecycles;
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
        server.stop(1);
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

                    if (!targetAvailable(exchange)) {
                        writeResponse(500, exchange);
                        return;
                    }

                    if (!consumerReady(consumerLoopLifecycles)) {
                        writeResponse(500, exchange);
                    }

                    writeResponse(200, exchange);
                }
            }
        );
    }

    private static boolean consumerReady(List<? extends IConsumerLoopLifecycle> consumerLoops) {
        return consumerLoops.stream().map(x -> x.ready()).anyMatch(y -> y.equals(true));
    }

    private static boolean targetAvailable(HttpExchange exchange) throws IOException {
        if (Config.TARGET_IS_ALIVE_HTTP_ENDPOINT != null) {
            try {
                var targetIsAliveResponse = client.send(
                    HttpRequest.newBuilder().uri(URI.create(Config.TARGET_IS_ALIVE_HTTP_ENDPOINT)).build(),
                    HttpResponse.BodyHandlers.ofString()
                );
                if (targetIsAliveResponse.statusCode() != 200) {
                    return false;
                }
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        }
        return true;
    }

    private static void writeResponse(int statusCode, HttpExchange exchange) throws IOException {
        final var os = exchange.getResponseBody();
        var responseText = (statusCode == 500 ? "false" : "true");
        exchange.sendResponseHeaders(statusCode, responseText.getBytes().length);
        os.write(responseText.getBytes());
        os.close();
    }
}
