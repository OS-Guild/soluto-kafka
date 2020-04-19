package monitoring;

import configuration.Config;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import reactor.kafka.receiver.ReceiverPartition;

public class Monitor {
    private static Counter processMessageStarted;
    private static Counter processMessageSuccess;
    private static Counter processMessageError;
    private static Counter retryProduced;
    private static Counter deadLetterProduced;
    private static Counter produceError;
    private static Counter targetExecutionRetry;
    private static Histogram messageLatency;
    private static Histogram processMessageExecutionTime;
    private static Histogram callTargetLatency;
    private static Histogram resultTargetLatency;

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

        messageLatency =
            Histogram
                .build()
                .buckets(buckets)
                .labelNames("topic")
                .name("message_latency")
                .help("message_latency")
                .register();

        callTargetLatency =
            Histogram.build().buckets(buckets).name("call_target_latency").help("call_target_latency").register();

        resultTargetLatency =
            Histogram.build().buckets(buckets).name("result_target_latency").help("result_target_latency").register();

        processMessageSuccess =
            Counter.build().name("process_message_success").help("process_message_success").register();

        processMessageError = Counter.build().name("process_message_error").help("process_message_error").register();

        processMessageExecutionTime =
            Histogram
                .build()
                .buckets(buckets)
                .name("process_message_execution_time")
                .help("process_message_execution_time")
                .register();

        processMessageStarted =
            Counter.build().name("process_message_started").help("process_message_started").register();

        retryProduced = Counter.build().name("retry_produced").help("retry_produced").register();

        deadLetterProduced = Counter.build().name("dead_letter_produced").help("dead_letter_produced").register();

        produceError = Counter.build().name("produce_error").help("produce_error").register();

        targetExecutionRetry =
            Counter
                .build()
                .name("target_execution_rerty")
                .labelNames("attempt")
                .help("target_execution_rerty")
                .register();
    }

    public static void receivedRecord(ConsumerRecord<String, String> record) {
        messageLatency.labels(record.topic()).observe(((double) (new Date().getTime() - record.timestamp())) / 1000);
        processMessageStarted.inc();
    }

    public static void callTargetLatency(long latency) {
        callTargetLatency.observe((double) latency / 1000);
    }

    public static void resultTargetLatency(long latency) {
        resultTargetLatency.observe((double) latency / 1000);
    }

    public static void processMessageSuccess(long executionStart) {
        processMessageExecutionTime.observe(((double) (new Date().getTime() - executionStart)) / 1000);
        processMessageSuccess.inc();
    }

    public static void processMessageError() {
        processMessageError.inc();
    }

    public static void retryProduced(ConsumerRecord<String, String> consumerRecord) {
        var extra = new JSONObject().put("message", new JSONObject().put("key", consumerRecord.key()));
        if (Config.LOG_RECORD) {
            extra.put("value", consumerRecord.value());
        }
        JSONObject log = new JSONObject().put("level", "info").put("message", "retry produced").put("extra", extra);
        write(log);

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

        deadLetterProduced.inc();
    }

    public static void unexpectedConsumerError(Throwable exception) {
        JSONObject log = new JSONObject()
            .put("level", "error")
            .put("message", "consumer stream was termineated due to unexpected error")
            .put(
                "err",
                new JSONObject().put("errorMessages", getErrorMessages(exception)).put("class", exception.getClass())
            );

        write(log);
    }

    public static void started() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-consumer-" + Config.GROUP_ID + " started");

        write(log);
    }

    public static void assignedToPartition(Collection<ReceiverPartition> partitions) {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "consumer was assigned to partitions")
            .put(
                "topicPartitions",
                partitions.stream().map(x -> x.topicPartition().toString()).collect(Collectors.joining(","))
            );
        write(log);
    }

    public static void revokedFromPartition(Collection<ReceiverPartition> partitions) {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "consumer was revoked from partitions")
            .put(
                "topicPartitions",
                partitions.stream().map(x -> x.topicPartition().toString()).collect(Collectors.joining(","))
            );
        write(log);
    }

    public static void backpressureBufferOverflow() {
        JSONObject log = new JSONObject().put("level", "info").put("message", "consumer backpressure overflow");
        write(log);
    }

    public static void serviceShutdown() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-consumer-" + Config.GROUP_ID + " shutdown");

        write(log);
    }

    public static void serviceTerminated() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-consumer-" + Config.GROUP_ID + " termindated");

        write(log);
    }

    public static void commitFailed(Throwable throwable) {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "commit failed")
            .put("err", new JSONObject().put("errorMessages", getErrorMessages(throwable)));

        write(log);
    }

    public static void produceError(
        String topicPrefix,
        ConsumerRecord<String, String> consumerRecord,
        Throwable exception
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

        targetExecutionRetry.labels(String.valueOf(attempt)).inc();
    }

    public static void consumerStreamCompleted() {
        JSONObject log = new JSONObject().put("level", "info").put("message", "consumer stream completed");

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

    public static void debug(String text) {
        if (!Config.DEBUG) return;
        JSONObject log = new JSONObject().put("level", "debug").put("message", text);

        write(log);
    }

    private static void write(JSONObject log) {
        System.out.println(log.toString());
    }

    private static ArrayList<String> getErrorMessagesArray(Throwable exception, ArrayList<String> messages) {
        if (exception == null) {
            return messages;
        }
        messages.add(exception.getMessage());
        return getErrorMessagesArray(exception.getCause(), messages);
    }

    private static JSONObject getErrorMessages(Throwable exception) {
        var messages = getErrorMessagesArray(exception, new ArrayList<String>());
        var errorMessages = new JSONObject();
        for (var i = 0; i < messages.size(); i++) {
            errorMessages.put("message" + i, messages.get(i));
        }
        return errorMessages;
    }
}