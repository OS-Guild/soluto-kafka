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
}