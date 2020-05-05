import configuration.*;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import kafka.*;
import monitoring.*;
import reactor.core.Disposable;
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
            System.out.println("target is alive 88888");

            monitoringServer = new MonitoringServer(targetIsAlive);

            consumer =
                ConsumerFactory
                    .create(monitoringServer)
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

            monitoringServer.start();
            Monitor.started();
            latch.await();
        } catch (Exception e) {
            Monitor.initializationError(e);
        }
        Monitor.serviceTerminated();
    }
}
