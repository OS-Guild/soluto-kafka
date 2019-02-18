import java.util.Date;

import com.google.common.collect.Iterators;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class Monitor {
    StatsDClient statsdClient;

    public Monitor(Config config) {
        if (config.JAVA_ENV.equals("production")) {
            statsdClient = new NonBlockingStatsDClient(config.STATSD_API_KEY + "." + config.STATSD_ROOT + ".kafka-consumer-"+ config.TOPIC + "-" + config.GROUP_ID, config.STATSD_HOST, 8125);
        }
    }

    public void consumed(ConsumerRecords<String, String> consumed) {
        if (statsdClient == null) return;
        statsdClient.recordGaugeValue("consumed", consumed.count());
    }

    
	public void consumedDedup(Iterable<ConsumerRecord<String, String>> records) {
        if (statsdClient == null) return;        
        statsdClient.recordGaugeValue("consumed-dedup", Iterators.size(records.iterator()));
	}

	public void messageLatency(ConsumerRecord<String, String> record) {
        if (statsdClient == null) return;        
        statsdClient.recordExecutionTime("message."+record.partition()+".latency", (new Date()).getTime() - record.timestamp());
	}

	public void process(long executionStart) {
        if (statsdClient == null) return;        
        statsdClient.recordExecutionTime("process.ExecutionTime", new Date().getTime() - executionStart);
	}
}