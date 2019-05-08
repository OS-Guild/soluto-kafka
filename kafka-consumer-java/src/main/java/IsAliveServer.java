import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class IsAliveServer {
    List<? extends IReady> consumerLoops;
    HttpServer server;

    public IsAliveServer(List<? extends IReady> consumerLoops) {
        this.consumerLoops = consumerLoops;
    }

    public IsAliveServer start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(Config.PORT), 0);
        isAliveGetRoute(server);
        server.start();
        return this;
    }

    public void close() {
        server.stop(1000);
    }

    private void isAliveGetRoute(HttpServer server) {
        var httpContext = server.createContext("/isAlive");
        httpContext.setHandler(new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (!exchange.getRequestMethod().equals("GET")) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }

                var response = consumerLoops
                    .stream()
                    .map(x -> x.ready())
                    .allMatch(y -> y.equals(true));

                var responseText = Boolean.toString(response);
                if (!response) {
                    exchange.sendResponseHeaders(500, responseText.getBytes().length);
                }
                else {
                    exchange.sendResponseHeaders(200, responseText.getBytes().length);
                }
                var os = exchange.getResponseBody();
                os.write(responseText.getBytes());
                os.close();
            }
        });        
    }
}

