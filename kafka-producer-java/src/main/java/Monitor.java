
import java.util.Date;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import org.json.JSONObject;

public class Monitor {
    StatsDClient statsdClient;
    Config config;

    public Monitor(Config config) {
        this.config = config;
        if (config.JAVA_ENV.equals("production")) {
            statsdClient = new NonBlockingStatsDClient(config.STATSD_API_KEY + "." + config.STATSD_ROOT + ".kafka-producer-"+ config.TOPIC + "." + config.PRODUCER_NAME + "." + config.CLUSTER, config.STATSD_HOST, 8125);
        }
    }

    public void produceLatency(long executionStart) {
        if (statsdClient == null) return; 
        var latency = (new Date()).getTime() - executionStart;
        System.out.println("produce.latency: " +  latency);
        statsdClient.recordExecutionTime("produce.latency", latency);
	}

    public void produceFail(Exception exception) {
        JSONObject log = new JSONObject()
        .put("level", "error")
        .put("message", "produce failed")
        .put("err", new JSONObject()
            .put("message", exception.getMessage()));

        output(log);
        
        if (statsdClient == null) return; 
        statsdClient.recordGaugeValue("produce.error", 1);
	}

	public void serviceStarted() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-producer-"+config.TOPIC+" started");

        output(log);
	}

	public void serviceShutdown() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-producer-"+config.TOPIC+" shutdown");

        output(log);
    }

    private void output(JSONObject log) {
        System.out.println(log.toString());
    }
}