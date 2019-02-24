import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

public class WriteLog {

    public void consumed(ConsumerRecords<String, String> consumed) {
        if (consumed.count() == 0) return;
        JSONObject log = new JSONObject()
        .put("level", "debug")
        .put("message", "consumed messages")
        .put("extra", new JSONObject()
            .put("count", consumed.count()));

        output(log);
    }

    public void unexpectedError(Exception exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "unexpected error")
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        output(log);
    }

	public void serviceStarted(Config config) {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+config.TOPIC+"-"+config.GROUP_ID + "started");

        output(log);
	}

	public void serviceShutdown(Config config) {
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
    
    public void deadLetterProducerError(ConsumerRecord<String, String> consumerRecord, Exception exception) {
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