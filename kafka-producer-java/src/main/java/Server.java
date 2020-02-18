import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Server {
    Config config;
    Monitor monitor;
    HttpServer server;
    Producer producer;

    Server(Config config, Monitor monitor, Producer producer) {
        this.config = config;
        this.monitor = monitor;
        this.producer = producer;
    }

    public Server start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(Config.PORT), 0);
        isAliveGetRoute(server);
        producePostRoute(server);
        server.start();
        return this;
    }

    public void close() {
        server.stop(5);
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
                    if (!producer.ready()) {
                        exchange.sendResponseHeaders(500, -1);
                        return;
                    }
                    exchange.sendResponseHeaders(204, -1);
                }
            }
        );
    }

    private void producePostRoute(HttpServer server) {
        var httpContext = server.createContext("/produce");
        httpContext.setHandler(
            new HttpHandler() {

                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    if (!exchange.getRequestMethod().equals("POST")) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }
                    try {
                        var body = CharStreams.toString(
                            new InputStreamReader(exchange.getRequestBody(), Charsets.UTF_8)
                        );
                        StreamSupport
                            .stream(new JSONArray(body).spliterator(), false)
                            .map(
                                jsonItem -> {
                                    var item = new JSONObject(jsonItem.toString());
                                    return new ProducerRequest(
                                        tryGetValue(item, "topic", null),
                                        tryGetValue(item, "key", null),
                                        tryGetValue(item, "message", "value")
                                    );
                                }
                            )
                            .map(producerRequest -> producer.produce(producerRequest))
                            .collect(Collectors.toList());

                        exchange.sendResponseHeaders(204, -1);
                    } catch (Exception e) {
                        var response = e.getMessage();
                        exchange.sendResponseHeaders(
                            e instanceof IllegalArgumentException ? 400 : 500,
                            response.getBytes().length
                        );
                        var os = exchange.getResponseBody();
                        os.write(response.getBytes());
                        os.close();
                    }
                }
            }
        );
    }

    private static String tryGetValue(JSONObject json, String option1, String option2) {
        String value;
        try {
            value = json.getString(option1);
        } catch (JSONException e1) {
            if (option2 == null) {
                throw new IllegalArgumentException(option1 + " is missing");
            }
            try {
                value = json.getString(option2);
            } catch (JSONException e2) {
                throw new IllegalArgumentException(option2 + " is missing");
            }
        }
        return value;
    }
}
