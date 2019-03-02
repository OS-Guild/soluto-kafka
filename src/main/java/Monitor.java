import java.util.Date;

import com.google.common.collect.Iterators;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

public class Monitor {
    StatsDClient statsdClient;
    Config config;

    public Monitor(Config config) {
        this.config = config;
        if (config.JAVA_ENV.equals("production")) {
            statsdClient = new NonBlockingStatsDClient(config.STATSD_API_KEY + "." + config.STATSD_ROOT + ".kafka-consumer-"+ config.TOPIC + "-" + config.GROUP_ID, config.STATSD_HOST, 8125);
        }
    }

    public void consumed(ConsumerRecords<String, String> consumed) {
        JSONObject log = new JSONObject()
        .put("level", "debug")
        .put("message", "consumed messages")
        .put("extra", new JSONObject()
            .put("count", consumed.count()));

        output(log);
        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("consumed", consumed.count());
    }

    public void consumedDedup(Iterable<ConsumerRecord<String, String>> records) {
        if (statsdClient == null) return;        
        statsdClient.recordGaugeValue("consumed-dedup", Iterators.size(records.iterator()));
    }
    
    public void messageLatency(ConsumerRecord<String, String> record) {
        if (statsdClient == null) return; 
        var latency = (new Date()).getTime() - record.timestamp();
        statsdClient.recordExecutionTime("message.latency", latency);
        statsdClient.recordExecutionTime("message."+record.partition()+".latency", latency);
    }

	public void processCompleted(long executionStart) {
        if (statsdClient == null) return;        
        statsdClient.recordExecutionTime("process.ExecutionTime", new Date().getTime() - executionStart);
	}

    public void deadLetterProduce() {
        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("deadLetterProduce", 1);
	}

    public void unexpectedError(Exception exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "unexpected error")
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        output(log);
    }

	public void serviceStarted() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+config.TOPIC+"-"+config.GROUP_ID + "started");

        output(log);
	}

	public void serviceShutdown() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+config.TOPIC+"-"+config.GROUP_ID + "shutdown");

        output(log);
    }

    public void commitFailed() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "commit failed, this usually indicates on consumer rebalancing");

        output(log);
    }

    public void deadLetterProduceError(ConsumerRecord<String, String> consumerRecord, Exception exception) {
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

	public void deadLetterProduce(ConsumerRecord<String, String> consumerRecord) {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "produced message to dead letter")
        .put("extra", new JSONObject()
            .put("message", new JSONObject()
                .put("key",consumerRecord.key()))
                .put("value", consumerRecord.value()));

        output(log);
    }

    private void output(JSONObject log) {
        System.out.println(log.toString());
    }
}