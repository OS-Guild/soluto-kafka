package monitoring;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import configuration.Config;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import target.TargetIsAlive;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MonitoringServer {
    private final TargetIsAlive targetIsAlive;
    private AdminClient client;
    private Consumer<?, ?> consumer;
    private boolean consumerAssigned;
    private boolean consumerDisposed;
    private HttpServer server;

    public MonitoringServer(TargetIsAlive targetIsAlive, AdminClient client) {
        this.targetIsAlive = targetIsAlive;
        this.client = client;
    }

    public MonitoringServer start() throws IOException {
        if (Config.MONITORING_SERVER_PORT == 0) {
            return this;
        }

        server = HttpServer.create(new InetSocketAddress(Config.MONITORING_SERVER_PORT), 0);
        isAliveGetRoute(server);
        if (Config.USE_PROMETHEUS) {
            DefaultExports.initialize();
            new HTTPServer(server, CollectorRegistry.defaultRegistry, false);
        } else {
            server.start();
        }
        return this;
    }

    public void consumerAssigned() {
        consumerAssigned = true;
    }

    public void consumerRevoked() {
        consumerAssigned = false;
    }

    public void consumerDisposed() {
        consumerDisposed = true;
    }

    public void close() {
        server.stop(0);
    }

    private void isAliveGetRoute(final HttpServer server) {
        final var httpContext = server.createContext("/isAlive");

        httpContext.setHandler(
                exchange -> {
                    if (!exchange.getRequestMethod().equals("GET")) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }

                    if (!consumerAssigned) {
                        writeResponse(500, exchange);
                        return;
                    }

                    if (consumerDisposed) {
                        writeResponse(500, exchange);
                        return;
                    }
                    var re = client.listPartitionReassignments().reassignments().get();

                    if (Monitor.getAssignedPartitions() == 0) {
                        writeResponse(500, exchange);
                        return;
                    }


                    if (!targetAlive(exchange)) {
                        writeResponse(500, exchange);
                        return;
                    }

                    writeResponse(200, exchange);
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
}
