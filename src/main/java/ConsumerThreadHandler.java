import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.TimeUnit;

public class ConsumerThreadHandler implements Runnable {

    private ConsumerRecord consumerRecord;

    public ConsumerThreadHandler(ConsumerRecord consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    public void run() {
        System.out.printf("offset = %d, partition = %d \n", consumerRecord.offset(), consumerRecord.partition());
        try {
            TimeUnit.MILLISECONDS.sleep(150);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}