import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.util.Date;
import org.json.JSONObject;

public class Monitor {
    static StatsDClient statsdClient;

    public static void init() {
        if (!Config.STATSD_CONFIGURED) {
            return;
        }

        statsdClient =
            new NonBlockingStatsDClient(
                Config.STATSD_API_KEY + "." + Config.STATSD_ROOT + "." + Config.STATSD_PRODUCER_NAME,
                Config.STATSD_HOST,
                8125
            );
    }

    public static void produceSuccess(ProducerRequest producerRequest, long executionStart) {
        JSONObject log = new JSONObject()
            .put("level", "debug")
            .put("message", "produce success")
            .put("extra", new JSONObject().put("topic", producerRequest.topic).put("key", producerRequest.key));

        output(log);

        if (statsdClient == null) return;
        statsdClient.recordGaugeValue(String.format("produce.%s.success", producerRequest.topic), 1);
        statsdClient.recordExecutionTime(
            String.format("produce.%s.latency", producerRequest.topic),
            (new Date()).getTime() - executionStart
        );
    }

    public static void produceFail(Exception exception) {
        JSONObject log = new JSONObject()
            .put("level", "error")
            .put("message", "produce failed")
            .put("err", new JSONObject().put("message", exception.getMessage()));

        output(log);

        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("produce.error", 1);
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
