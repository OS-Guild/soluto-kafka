public interface IConsumerRunnerLifecycle {
    boolean assignedToPartition();
    void stop();
    void start();
}
