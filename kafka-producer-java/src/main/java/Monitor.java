
import java.util.Date;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import org.json.JSONObject;

public class Monitor {
    static StatsDClient statsdClient;

    public static void init() {
        if (Config.STATSD_MONITOR) {
            statsdClient = new NonBlockingStatsDClient(Config.STATSD_API_KEY + "." + Config.STATSD_ROOT + "." + Config.STATSD_PRODUCER_NAME, Config.STATSD_HOST, 8125);
        }
    }

    public static void produceLatency(long executionStart) {
        if (statsdClient == null) return; 
        var latency = (new Date()).getTime() - executionStart;
        System.out.println("produce.latency: " +  latency);
        statsdClient.recordExecutionTime("produce.latency", latency);
	}

    public static void produceFail(Exception exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "produce failed")
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        output(log);
        
        if (statsdClient == null) return; 
        statsdClient.recordGaugeValue("produce.error", 1);
	}

	public static void started() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-producer-"+Config.TOPIC+" started");

        output(log);
    }
    
    public static void ready() {
        JSONObject log = new JSONObject()
        .put("level", "info")
        .put("message", "kafka-producer-"+Config.TOPIC+" ready");

        output(log);
	}

	public static void serviceShutdown() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-producer-"+Config.TOPIC+" shutdown");

        output(log);
    }

    private static void output(JSONObject log) {
        System.out.println(log.toString());
    }
}