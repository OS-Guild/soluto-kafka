import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class ManagementServer {
    List<? extends IConsumerLoopLifecycle> consumerLoops;
    HttpServer server;

    public ManagementServer(List<? extends IConsumerLoopLifecycle> consumerLoops) {
        this.consumerLoops = consumerLoops;
    }

    public ManagementServer start() throws IOException {
        if (Config.MANAGEMENT_SERVER_PORT != 0) {
            server = HttpServer.create(new InetSocketAddress(Config.MANAGEMENT_SERVER_PORT), 0);
            isAliveGetRoute(server);
            if (Config.USE_PROMETHEUS) {
                DefaultExports.initialize();
                new HTTPServer(server, CollectorRegistry.defaultRegistry, false);
            } else {
                server.start();
            }
        }
        return this;
    }

    public void close() {
        server.stop(1);
    }

    private void isAliveGetRoute(HttpServer server) {
        var httpContext = server.createContext("/isAlive");
        httpContext.setHandler(
            new HttpHandler() {

                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    if (!exchange.getRequestMethod().equals("GET")) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }

                    var response = consumerLoops.stream().map(x -> x.ready()).anyMatch(y -> y.equals(true));

                    var responseText = Boolean.toString(response);
                    if (!response) {
                        exchange.sendResponseHeaders(500, responseText.getBytes().length);
                    } else {
                        exchange.sendResponseHeaders(200, responseText.getBytes().length);
                    }
                    var os = exchange.getResponseBody();
                    os.write(responseText.getBytes());
                    os.close();
                }
            }
        );
    }
}
