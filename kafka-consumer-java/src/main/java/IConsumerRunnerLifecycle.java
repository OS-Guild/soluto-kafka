public interface IConsumerRunnerLifecycle {
    void start();
    void stop();
    boolean ready();
}
