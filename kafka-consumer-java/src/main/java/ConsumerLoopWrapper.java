import java.util.concurrent.CountDownLatch;

public class ConsumerLoopWrapper implements Runnable, IConsumerLoopLifecycle {
    ConsumerLoop consumerLoop;
    CountDownLatch countDownLatch;

    public ConsumerLoopWrapper(ConsumerLoop consumerLoop, CountDownLatch countDownLatch) {
        this.consumerLoop = consumerLoop;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        this.consumerLoop.run();
        this.countDownLatch.countDown();
    }

    @Override
    public boolean assignedToPartition() {
        return this.consumerLoop.assignedToPartition();
    }

    @Override
    public void stop() {
        consumerLoop.stop();
    }
}
