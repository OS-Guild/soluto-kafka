public interface IConsumerLoopLifecycle {
    public boolean assignedToPartition();
    public void stop();
}
