package monitoring;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import configuration.Config;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import io.reactivex.disposables.Disposable;
import java.io.IOException;
import java.net.InetSocketAddress;
import target.TargetIsAlive;

public class MonitoringServer {
    private Disposable consumer;
    private boolean consumerAssigned;
    private final TargetIsAlive targetIsAlive;
    private HttpServer server;

    public MonitoringServer(Disposable consumer, TargetIsAlive targetIsAlive) {
        this.targetIsAlive = targetIsAlive;
        this.consumer = consumer;
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

                    if (consumer.isDisposed()) {
                        writeResponse(500, exchange);
                        return;
                    }

                    if (!consumerAssigned) {
                        writeResponse(500, exchange);
                    }

                    writeResponse(200, exchange);
                }
            }
        );
    }

    private boolean targetAlive(HttpExchange exchange) throws IOException {
        if (Config.TARGET_IS_ALIVE_HTTP_ENDPOINT != null) {
            try {
                return this.targetIsAlive.check();
            } catch (Exception e) {
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

    public void consumerAssigned() {
        consumerAssigned = true;
    }
}