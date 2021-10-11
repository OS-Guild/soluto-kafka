import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Server {
    Config config;
    Monitor monitor;
    HttpServer server;
    AbstractProducer producer;

    Server(Config config, Monitor monitor, AbstractProducer producer) {
        this.config = config;
        this.monitor = monitor;
        this.producer = producer;
    }

    public Server start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(Config.PORT), 0);
        isAliveGetRoute(server);
        producePostRoute(server);
        if (Config.USE_PROMETHEUS) {
            DefaultExports.initialize();
            new HTTPServer(server, CollectorRegistry.defaultRegistry, false);
        } else {
            server.start();
        }
        return this;
    }

    public void close() {
        server.stop(0);
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
                    var body = CharStreams.toString(new InputStreamReader(exchange.getRequestBody(), Charsets.UTF_8));
                    try {
                        List<ProducerRequest> requests = StreamSupport
                            .stream(new JSONArray(body).spliterator(), false)
                            .map(
                                jsonItem -> {
                                    var item = new JSONObject(jsonItem.toString());
                                    return new ProducerRequest(
                                        tryGetValue(item, "topic", null),
                                        tryGetValue(item, "key", null),
                                        tryGetValue(item, "message", "value"),
                                        tryGetHeaders(item)
                                    );
                                }
                            )
                            .collect(Collectors.toList());
                        for (ProducerRequest producerRequest : requests) {
                            producer.produce(producerRequest);
                        }
                        exchange.sendResponseHeaders(204, -1);
                    } catch (Exception e) {
                        e.printStackTrace();
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

    private Iterable<Header> tryGetHeaders(JSONObject item) {
        if (item.has("headers")) {
            JSONObject headersJson = item.getJSONObject("headers");
            Iterator<String> keys = headersJson.keys();
            if (keys.hasNext()) {
                RecordHeaders headers = new RecordHeaders();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if (headersJson.has(key)) {
                        try {
                            headers.add(key, headersJson.getString(key).getBytes());
                        } catch (JSONException e) {
                            headers.add(key, null);
                        }
                    }
                }
                return headers;
            }
        }
        return null;
    }

    private static String tryGetValue(JSONObject json, String option1, String option2) {
        if (json.has(option1)) {
            return json.get(option1).toString();
        }
        if (json.has(option2)) {
            return json.get(option2).toString();
        }

        if (option2 == null) {
            throw new IllegalArgumentException(option1 + " is missing");
        }
        throw new IllegalArgumentException(option2 + " is missing");
    }
}
