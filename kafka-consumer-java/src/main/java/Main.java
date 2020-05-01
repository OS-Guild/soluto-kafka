import configuration.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.plugins.RxJavaPlugins;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import kafka.*;
import monitoring.*;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import target.*;

public class Main {
    static Disposable consumer;
    static MonitoringServer monitoringServer;
    static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {
        try {
            Config.init();
            Monitor.init();

            var targetIsAlive = new TargetIsAlive();
            do {
                System.out.println("waiting for target to be alive");
                Thread.sleep(1000);
            } while (!targetIsAlive.check());
            System.out.println("target is alive");

            monitoringServer = new MonitoringServer(targetIsAlive);
            consumer =
                ConsumerFactory
                    .create(
                        KafkaReceiver.create(
                            ReceiverOptions
                                .<String, String>create(KafkaOptions.consumer())
                                .subscription(Config.TOPICS)
                                .commitInterval(Duration.ofMillis(Config.COMMIT_INTERVAL))
                                .addAssignListener(
                                    partitions -> {
                                        monitoringServer.consumerAssigned();
                                        Monitor.assignedToPartition(partitions);
                                    }
                                )
                                .addRevokeListener(
                                    partitions -> {
                                        Monitor.revokedFromPartition(partitions);
                                    }
                                )
                                .pollTimeout(Duration.ofMillis(Config.POLL_TIMEOUT))
                        )
                    )
                    .stream()
                    .subscribe(
                        __ -> {},
                        exception -> {
                            monitoringServer.consumerDisposed();
                            Monitor.consumerError(exception);
                        },
                        () -> {
                            monitoringServer.consumerDisposed();
                            Monitor.consumerCompleted();
                        }
                    );

            Runtime
                .getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> {
                            System.out.println("Shutting down");
                            consumer.dispose();
                            monitoringServer.close();
                            latch.countDown();
                        }
                    )
                );
            RxJavaPlugins.setErrorHandler(e -> Monitor.unexpectedError(e));

            monitoringServer.start();
            Monitor.started();
            latch.await();
        } catch (Exception e) {
            Monitor.initializationError(e);
        }
        Monitor.serviceTerminated();
    }
}
