import com.google.common.collect.Iterators;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

public class Monitor {
    static StatsDClient statsdClient;
    static Histogram messageLatencyHistogram;

    public static void init() {
        if (Config.STATSD_CONFIGURED) {
            statsdClient =
                new NonBlockingStatsDClient(
                    Config.STATSD_API_KEY + "." + Config.STATSD_ROOT + "." + Config.STATSD_CONSUMER_NAME,
                    Config.STATSD_HOST,
                    8125
                );
        }
        if (Config.USE_PROMETHEUS) {
            messageLatencyHistogram =
                Histogram
                    .build()
                    .buckets(3, 30, 100, 300, 1500, 10000)
                    .name("message_latency")
                    .help("message_latency")
                    .register();
        }
    }

    public static void consumed(ConsumerRecords<String, String> consumed) {
        JSONObject log = new JSONObject()
            .put("level", "debug")
            .put("message", "consumed messages")
            .put("extra", new JSONObject().put("count", consumed.count()));

        write(log);
        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("consumed", consumed.count());
    }

    public static void consumedPartitioned(Iterable<Iterable<ConsumerRecord<String, String>>> partitions) {
        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("consumed-partitions", Iterators.size(partitions.iterator()));
    }

    public static void messageLatency(ConsumerRecord<String, String> record) {
        var latency = (new Date()).getTime() - record.timestamp();
        if (statsdClient != null) {
            statsdClient.recordExecutionTime("message.latency", latency);
            statsdClient.recordExecutionTime("message." + record.partition() + ".latency", latency);
        }
        if (messageLatencyHistogram != null) {
            messageLatencyHistogram.observe(latency);
        }
    }

    public static void callTargetLatency(long latency) {
        if (statsdClient == null) return;
        statsdClient.recordExecutionTime("callTarget.latency", latency);
    }

    public static void resultTargetLatency(long latency) {
        if (statsdClient == null) return;
        statsdClient.recordExecutionTime("resultTarget.latency", latency);
    }

    public static void processCompleted(long executionStart) {
        if (statsdClient == null) return;
        statsdClient.recordExecutionTime("process.ExecutionTime", new Date().getTime() - executionStart);
    }

    public static void processMessageCompleted(long executionStart) {
        if (statsdClient == null) return;
        statsdClient.recordExecutionTime("processMessage.ExecutionTime", new Date().getTime() - executionStart);
    }

    public static void topicProduced(String topicPrefix, ConsumerRecord<String, String> consumerRecord) {
        JSONObject log = new JSONObject()
            .put("level", "error")
            .put("message", String.format("%s produced", topicPrefix))
            .put(
                "extra",
                new JSONObject()
                    .put("message", new JSONObject().put("key", consumerRecord.key()))
                    .put("value", (!Config.HIDE_CONSUMED_MESSAGE) ? consumerRecord.value() : "Hidden")
            );

        write(log);

        if (statsdClient == null) return;
        statsdClient.incrementCounter(String.format("%sProduced", topicPrefix));
    }

    public static void unexpectedError(Exception exception) {
        var messages = getAllErrorMessages(exception, new ArrayList<String>());
        var errorMessages = new JSONObject();
        for (var i = 0; i < messages.size(); i++) {
            errorMessages.put("message" + i, messages.get(i));
        }

        JSONObject log = new JSONObject()
            .put("level", "error")
            .put("message", "unexpected error")
            .put(
                "err",
                new JSONObject()
                    .put("errorMessages", errorMessages)
                    .put("class", exception.getClass())
                    .put("stacktrace", exception.getStackTrace())
                    .put("innerExceptionMesssage", exception.getCause().getMessage())
            );

        write(log);
    }

    public static void started() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-consumer-" + Config.TOPIC + "-" + Config.GROUP_ID + " started");

        write(log);
    }

    public static void ready(int id) {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-consumer-" + id + "-" + Config.TOPIC + "-" + Config.GROUP_ID + " ready");

        write(log);
    }

    public static void serviceShutdown() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-consumer-" + Config.TOPIC + "-" + Config.GROUP_ID + "shutdown");

        write(log);
    }

    public static void serviceTerminated() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-consumer-" + Config.TOPIC + "-" + Config.GROUP_ID + " termindated");

        write(log);
    }

    public static void commitFailed() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "commit failed, this usually indicates on consumer rebalancing");

        write(log);
    }

    public static void produceError(
        String topicPrefix,
        ConsumerRecord<String, String> consumerRecord,
        Exception exception
    ) {
        JSONObject log = new JSONObject()
            .put("level", "error")
            .put("message", String.format("failed producing message to %s topic", topicPrefix))
            .put(
                "extra",
                new JSONObject()
                    .put("message", new JSONObject().put("key", consumerRecord.key()))
                    .put("value", (!Config.HIDE_CONSUMED_MESSAGE) ? consumerRecord.value() : "Hidden")
            )
            .put("err", new JSONObject().put("message", exception.getMessage()));

        write(log);
        if (statsdClient == null) return;
        statsdClient.incrementCounter(String.format("%sProduceError", topicPrefix));
    }

    public static void targetExecutionRetry(
        ConsumerRecord<String, String> consumerRecord,
        Optional<String> responseBody,
        Throwable exception,
        int attempt
    ) {
        JSONObject log = new JSONObject().put("level", "info").put("message", "target retry");

        var extra = new JSONObject()
            .put(
                "message",
                new JSONObject()
                    .put("key", consumerRecord.key())
                    .put("value", (!Config.HIDE_CONSUMED_MESSAGE) ? consumerRecord.value() : "Hidden")
            )
            .put("attempt", attempt);

        if (responseBody.isPresent()) {
            extra.put("response", responseBody.get());
        }

        var error = new JSONObject();
        if (exception != null) {
            error.put("message", exception.getMessage());
            error.put("type", exception.getClass());
        }

        log.put("extra", extra);
        log.put("err", error);

        write(log);

        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("targetExecutionRetry." + attempt, 1);
    }

    public static void targetConnectionUnavailable() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "target connection unavailable, terminating consumer");

        write(log);
    }

    public static void debug(String text) {
        if (!Config.DEBUG) return;
        JSONObject log = new JSONObject().put("level", "debug").put("message", text);

        write(log);
    }

    private static void write(JSONObject log) {
        System.out.println(log.toString());
    }

    private static ArrayList<String> getAllErrorMessages(Throwable exception, ArrayList<String> messages) {
        messages.add(exception.getMessage());
        if (exception.getCause() == null) {
            return messages;
        }
        return getAllErrorMessages(exception.getCause(), messages);
    }
}
