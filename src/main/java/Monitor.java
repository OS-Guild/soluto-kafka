import java.net.http.HttpResponse;
import java.util.Date;

import com.google.common.collect.Iterators;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

public class Monitor {
    static StatsDClient statsdClient;

    public static void init() {
        if (Config.JAVA_ENV.equals("production")) {
            statsdClient = new NonBlockingStatsDClient(Config.STATSD_API_KEY + "." + Config.STATSD_ROOT + ".kafka-consumer-"+ Config.TOPIC + "-" + Config.GROUP_ID, Config.STATSD_HOST, 8125);
        }
    }

    public static void consumed(ConsumerRecords<String, String> consumed) {
        JSONObject log = new JSONObject()
        .put("level", "debug")
        .put("message", "consumed messages")
        .put("extra", new JSONObject()
            .put("count", consumed.count()));

        write(log);
        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("consumed", consumed.count());
    }

    public static void consumedDedup(Iterable<ConsumerRecord<String, String>> records) {
        if (statsdClient == null) return;        
        statsdClient.recordGaugeValue("consumed-dedup", Iterators.size(records.iterator()));
    }
    
    public static void messageLatency(ConsumerRecord<String, String> record) {
        if (statsdClient == null) return; 
        var latency = (new Date()).getTime() - record.timestamp();
        statsdClient.recordExecutionTime("message.latency", latency);
        statsdClient.recordExecutionTime("message."+record.partition()+".latency", latency);
    }

	public static void processCompleted(long executionStart) {
        if (statsdClient == null) return;        
        statsdClient.recordExecutionTime("process.ExecutionTime", new Date().getTime() - executionStart);
	}

    public static void deadLetterProduced(ConsumerRecord<String, String> consumerRecord) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "target execution failed - dead letter produced")
        .put("extra", new JSONObject()
            .put("message", new JSONObject()
                .put("key",consumerRecord.key()))
                .put("value", consumerRecord.value()));

        write(log);

        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("deadLetterProduced", 1);
	}

    public static void unexpectedError(Exception exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "unexpected error")
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        write(log);
    }

	public static void serviceStarted() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+Config.TOPIC+"-"+Config.GROUP_ID + " started");

        write(log);
    }
    
    public static void consumerReady(int id) {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+id+"-"+Config.TOPIC+"-"+Config.GROUP_ID + " ready");

        write(log);
	}

	public static void serviceShutdown() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+Config.TOPIC+"-"+Config.GROUP_ID + "shutdown");

        write(log);
    }

    public static void commitFailed() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "commit failed, this usually indicates on consumer rebalancing");

        write(log);
    }

    public static void deadLetterProduceError(ConsumerRecord<String, String> consumerRecord, Exception exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "target execution failed - failed producing message to dead letter")
        .put("extra", new JSONObject()
            .put("message", new JSONObject()
                .put("key",consumerRecord.key()))
                .put("value", consumerRecord.value()))
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        write(log);
        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("deadLetterProduceError", 1);
    }

	public static void targetExecutionRetry(ConsumerRecord<String, String> consumerRecord, HttpResponse<String> response, Throwable exception, int attempt) {
        JSONObject log = new JSONObject()
        .put("level", "warning")
        .put("message", "target execution failed - retrying");

        var extra = new JSONObject()
            .put("message", new JSONObject()
                .put("key",consumerRecord.key())
                .put("value", consumerRecord.value())
            )
            .put("attempt", attempt);
        
        if (response != null) {
            extra.put("response", response.body());
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
    
    public static void targetUnavailable() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "target unavailable due to connection error");

        write(log);
    }

    private static void write(JSONObject log) {
        System.out.println(log.toString());
    }
}