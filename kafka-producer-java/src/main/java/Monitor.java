import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.Arrays;
import java.util.Date;
import org.apache.commons.lang.ArrayUtils;
import org.json.JSONObject;

public class Monitor {
    private static Counter produceSuccess;
    private static Counter produceError;
    private static Histogram produceLatency;

    private static double[] buckets = ArrayUtils.toPrimitive(
        Arrays
            .asList(Config.PROMETHEUS_BUCKETS.split(","))
            .stream()
            .map(s -> Double.parseDouble(s))
            .toArray(Double[]::new)
    );

    public static void init() {
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
        JSONObject log = new JSONObject()
            .put("level", "debug")
            .put("message", "produce success")
            .put("extra", new JSONObject().put("topic", producerRequest.topic).put("key", producerRequest.key));

        output(log);

        produceSuccess.labels(producerRequest.topic).inc();
        produceLatency.labels(producerRequest.topic).observe(((double) (new Date().getTime() - executionStart)) / 1000);
    }

    public static void produceError(Exception exception) {
        JSONObject log = new JSONObject()
            .put("level", "error")
            .put("message", "produce failed")
            .put("err", new JSONObject().put("message", exception.getMessage()));

        output(log);

        produceError.inc();
    }

    public static void started() {
        JSONObject log = new JSONObject().put("level", "info").put("message", "kafka-producer started");

        output(log);
    }

    public static void ready() {
        JSONObject log = new JSONObject().put("level", "info").put("message", "kafka-producer ready");

        output(log);
    }

    public static void serviceShutdown() {
        JSONObject log = new JSONObject().put("level", "info").put("message", "kafka-producer shutdown");

        output(log);
    }

    private static void output(JSONObject log) {
        System.out.println(log.toString());
    }
}
