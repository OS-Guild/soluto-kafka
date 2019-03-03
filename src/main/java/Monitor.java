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

        output(log);
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

    public static void deadLetterProduce() {
        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("deadLetterProduce", 1);
	}

    public static void unexpectedError(Exception exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "unexpected error")
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        output(log);
    }

	public static void serviceStarted() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+Config.TOPIC+"-"+Config.GROUP_ID + " started");

        output(log);
    }
    
    public static void consumerReady(int id) {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+id+"-"+Config.TOPIC+"-"+Config.GROUP_ID + " ready");

        output(log);
	}

	public static void serviceShutdown() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+Config.TOPIC+"-"+Config.GROUP_ID + "shutdown");

        output(log);
    }

    public static void commitFailed() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "commit failed, this usually indicates on consumer rebalancing");

        output(log);
    }

    public static void deadLetterProduceError(ConsumerRecord<String, String> consumerRecord, Exception exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "failed producing message to dead letter")
        .put("extra", new JSONObject()
            .put("message", new JSONObject()
                .put("key",consumerRecord.key()))
                .put("value", consumerRecord.value()))
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        output(log);
        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("deadLetterProduce.error", 1);
    }

	public static void deadLetterProduce(ConsumerRecord<String, String> consumerRecord) {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "produced message to dead letter")
        .put("extra", new JSONObject()
            .put("message", new JSONObject()
                .put("key",consumerRecord.key()))
                .put("value", consumerRecord.value()));

        output(log);
    }

    public static void sendHttpReqeustError(ConsumerRecord<String, String> consumerRecord, Throwable exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "failed sending http request")
        .put("extra", new JSONObject()
            .put("message", new JSONObject()
                .put("key",consumerRecord.key()))
                .put("value", consumerRecord.value()))
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        output(log);

        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("sendHttpReqeust.error", 1);
    }

    private static void output(JSONObject log) {
        System.out.println(log.toString());
    }
}