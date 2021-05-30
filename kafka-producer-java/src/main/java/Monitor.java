import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.Arrays;
import java.util.Date;
import org.json.JSONObject;

public class Monitor {
    private static Counter produceSuccess;
    private static Counter produceError;
    private static Histogram produceLatency;

    private static double[] buckets = new double[0];

    public static void init() {
        if (Config.PROMETHEUS_BUCKETS != null) {
            buckets =
                Arrays
                    .asList(Config.PROMETHEUS_BUCKETS.split(","))
                    .stream()
                    .mapToDouble(s -> Double.parseDouble(s))
                    .toArray();
        }

        produceSuccess = Counter.build().name("produce_success").labelNames("topic").help("produce_success").register();
        produceError = Counter.build().name("produce_error").help("produce_error").register();

        produceLatency =
            Histogram
                .build()
                .buckets(buckets)
                .name("produce_latency")
                .labelNames("topic")
                .help("produce_latency")
                .register();
    }

    public static void produceSuccess(ProducerRequest producerRequest, long executionStart) {
        produceSuccess.labels(producerRequest.topic).inc();
        produceLatency.labels(producerRequest.topic).observe(((double) (new Date().getTime() - executionStart)) / 1000);

        var headers = new JSONObject();
        producerRequest.headers.forEach(
            header -> {
                headers.put(header.key(), new String(header.value()));
            }
        );

        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "produce success")
            .put(
                "extra",
                new JSONObject()
                    .put("topic", producerRequest.topic)
                    .put("key", producerRequest.key)
                    .put("headers", headers)
            );

        write(log);
    }

    public static void produceError(Exception exception) {
        JSONObject log = new JSONObject()
            .put("level", "error")
            .put("message", "produce failed")
            .put("err", new JSONObject().put("message", exception.getMessage()));

        write(log);

        produceError.inc();
    }

    public static void started() {
        JSONObject log = new JSONObject().put("level", "info").put("message", "kafka-producer started");
        write(log);
    }

    public static void ready() {
        JSONObject log = new JSONObject().put("level", "info").put("message", "kafka-producer ready");
        write(log);
    }

    public static void serviceShutdown() {
        JSONObject log = new JSONObject().put("level", "info").put("message", "kafka-producer shutdown");
        write(log);
    }

    private static void write(JSONObject log) {
        System.out.println(log.toString());
    }
}
