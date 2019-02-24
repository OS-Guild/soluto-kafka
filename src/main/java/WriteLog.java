import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

public class WriteLog {

    public void consumed(ConsumerRecords<String, String> consumed) {
        if (consumed.count() > 0) {
            JSONObject log = new JSONObject()
            .put("level", "debug")
            .put("message", "consumed messages")
            .put("extra", new JSONObject()
                .put("count", consumed.count()));
    
            System.out.println(log.toString());
        }
    }

    public void unexpectedErorr(Exception exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "unexpected error")
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        System.out.println(log.toString());

    }

	public void serviceStarted(Config config) {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+config.TOPIC+"-"+config.GROUP_ID + "started");

        System.out.println(log.toString());
	}

	public void serviceShutdown(Config config) {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-consumer-"+config.TOPIC+"-"+config.GROUP_ID + "shutdown");

        System.out.println(log.toString());
    }
    
    public void commitFailed() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "commit failed, this usually indicates on consumer rebalancing");

        System.out.println(log.toString());
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

        System.out.println(log.toString());
	}

	public void deadLetterProduce(ConsumerRecord<String, String> consumerRecord) {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "produced message to dead letter")
        .put("extra", new JSONObject()
            .put("message", new JSONObject()
                .put("key",consumerRecord.key()))
                .put("value", consumerRecord.value()));

        System.out.println(log.toString());
	}
}