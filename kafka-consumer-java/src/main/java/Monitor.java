import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

public class Monitor {
    private static Counter processMessageStarted;
    private static Counter processMessageSuccess;
    private static Counter processMessageError;
    private static Counter processBatchCompleted;
    private static Counter consumed;
    private static Counter retryProduced;
    private static Counter deadLetterProduced;
    private static Counter produceError;
    private static Counter targetExecutionRetry;
    private static Histogram messageLatency;
    private static Histogram processMessageExecutionTime;
    private static Histogram processExecutionTime;
    private static Histogram callTargetLatency;
    private static Histogram resultTargetLatency;

    private static double[] buckets = ArrayUtils.toPrimitive(
        Arrays
            .asList(Config.PROMETHEUS_BUCKETS.split(","))
            .stream()
            .map(s -> Double.parseDouble(s))
            .toArray(Double[]::new)
    );

    public static void consumed(ConsumerRecords<String, String> records) {
        JSONObject log = new JSONObject()
            .put("level", "debug")
            .put("message", "consumed messages")
            .put("extra", new JSONObject().put("count", records.count()));

        write(log);

        if (consumed == null) {
            consumed = Counter.build().name("consumed").help("consumed").register();
        }
        consumed.inc(records.count());
    }

    public static void messageLatency(ConsumerRecord<String, String> record) {
        if (messageLatency == null) {
            messageLatency =
                Histogram.build().buckets(buckets).name("message_latency").help("message_latency").register();
        }
        messageLatency.observe(((new Date()).getTime() - record.timestamp()) / 1000);
    }

    public static void callTargetLatency(long latency) {
        if (callTargetLatency == null) {
            callTargetLatency =
                Histogram.build().buckets(buckets).name("call_target_latency").help("call_target_latency").register();
        }
        callTargetLatency.observe(latency);
    }

    public static void resultTargetLatency(long latency) {
        if (resultTargetLatency == null) {
            resultTargetLatency =
                Histogram.build().buckets(buckets).name("call_target_latency").help("call_target_latency").register();
        }
        resultTargetLatency.observe(latency);
    }

    public static void processBatchCompleted(long executionStart) {
        if (processBatchCompleted == null) {
            Counter.build().name("process_batch_completed").help("process_batch_completed").register();
        }
        if (processExecutionTime == null) {
            processExecutionTime =
                Histogram
                    .build()
                    .buckets(buckets)
                    .name("process_execution_time")
                    .help("process_execution_time")
                    .register();
        }
        processBatchCompleted.inc();
        processExecutionTime.observe(((new Date().getTime() - executionStart)) / 1000);
    }

    public static void processMessageStarted() {
        if (processMessageStarted == null) {
            processMessageStarted =
                Counter.build().name("process_message_started").help("process_message_started").register();
        }
        processMessageStarted.inc();
    }

    public static void processMessageSuccess(long executionStart) {
        if (processMessageSuccess == null) {
            processMessageSuccess =
                Counter.build().name("process_message_success").help("process_message_success").register();
        }
        if (processMessageExecutionTime == null) {
            processMessageExecutionTime =
                Histogram
                    .build()
                    .buckets(buckets)
                    .name("process_message_execution_time")
                    .help("process_message_execution_time")
                    .register();
        }

        processMessageExecutionTime.observe((new Date().getTime() - executionStart) / 1000);
        processMessageSuccess.inc();
    }

    public static void processMessageFailed() {
        if (processMessageError == null) {
            processMessageError =
                Counter.build().name("process_message_error").help("process_message_error").register();
        }
        processMessageError.inc();
    }

    public static void retryProduced(ConsumerRecord<String, String> consumerRecord) {
        var extra = new JSONObject().put("message", new JSONObject().put("key", consumerRecord.key()));
        if (Config.LOG_RECORD) {
            extra.put("value", consumerRecord.value());
        }
        JSONObject log = new JSONObject().put("level", "info").put("message", "retry produced").put("extra", extra);
        write(log);

        if (retryProduced == null) {
            retryProduced = Counter.build().name("retry_produced").help("retry_produced").register();
        }
        retryProduced.inc();
    }

    public static void deadLetterProcdued(ConsumerRecord<String, String> consumerRecord) {
        var extra = new JSONObject().put("message", new JSONObject().put("key", consumerRecord.key()));
        if (Config.LOG_RECORD) {
            extra.put("value", consumerRecord.value());
        }
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "dead letter produced")
            .put("extra", extra);

        write(log);

        if (deadLetterProduced == null) {
            deadLetterProduced = Counter.build().name("dead_letter_produced").help("dead_letter_produced").register();
            deadLetterProduced.inc();
        }
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
            );

        write(log);
    }

    public static void started() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-consumer-" + Config.TOPIC + "-" + Config.GROUP_ID + " started");

        write(log);
    }

    public static void assignedToPartition(int id) {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put(
                "message",
                "kafka-consumer-" + id + "-" + Config.TOPIC + "-" + Config.GROUP_ID + " was assigned to partitions"
            );

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
        var extra = new JSONObject().put("message", new JSONObject().put("key", consumerRecord.key()));
        if (Config.LOG_RECORD) {
            extra.put("value", consumerRecord.value());
        }
        JSONObject log = new JSONObject()
            .put("level", "error")
            .put("message", String.format("failed producing message to %s topic", topicPrefix))
            .put("extra", extra)
            .put("err", new JSONObject().put("message", exception.getMessage()));

        write(log);

        if (produceError == null) {
            produceError = Counter.build().name("produce_error").help("produce_error").register();
        }
        produceError.inc();
    }

    public static void targetExecutionRetry(
        ConsumerRecord<String, String> consumerRecord,
        Optional<String> responseBody,
        Throwable exception,
        int attempt
    ) {
        var extra = new JSONObject();
        extra.put("message", new JSONObject().put("key", consumerRecord.key()));
        if (Config.LOG_RECORD) {
            extra.put("value", consumerRecord.value());
        }
        if (responseBody.isPresent()) {
            extra.put("response", responseBody.get());
        }

        var error = new JSONObject();
        if (exception != null) {
            error.put("message", exception.getMessage());
            error.put("type", exception.getClass());
        }

        JSONObject log = new JSONObject().put("level", "info").put("message", "target retry");

        log.put("extra", extra);
        log.put("err", error);

        write(log);

        if (targetExecutionRetry == null) {
            targetExecutionRetry =
                Counter
                    .build()
                    .name("target_execution_rerty")
                    .labelNames("attempt")
                    .help("target_execution_rerty")
                    .register();
        }
        targetExecutionRetry.labels(String.valueOf(attempt)).inc();
    }

    public static void targetConnectionUnavailable() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "target connection unavailable, terminating consumer");

        write(log);
    }

    public static void targetNotAlive(int targetIsAliveStatusCode) {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "target not alive")
            .put("targetIsAliveStatusCode", targetIsAliveStatusCode);

        write(log);
    }

    public static void targetAlive(int targetIsAliveStatusCode) {
        if (Config.DEBUG) {
            JSONObject log = new JSONObject()
                .put("level", "info")
                .put("message", "target alive")
                .put("targetIsAliveStatusCode", targetIsAliveStatusCode);

            write(log);
        }
    }

    public static void consumerNotAssignedToAtLeastOnePartition() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "none of the consumer threads are assigned to a partition");

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
        if (exception == null) {
            return messages;
        }
        messages.add(exception.getMessage());
        return getAllErrorMessages(exception.getCause(), messages);
    }
}
