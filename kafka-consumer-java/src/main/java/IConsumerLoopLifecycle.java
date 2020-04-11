public interface IConsumerLoopLifecycle {
    boolean assignedToPartition();
    void stop();
    void start();
}
