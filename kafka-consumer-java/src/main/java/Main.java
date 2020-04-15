import configuration.*;
import io.reactivex.disposables.Disposable;
import java.util.concurrent.CountDownLatch;
import kafka.*;
import monitoring.*;
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

            monitoringServer = new MonitoringServer(consumer, targetIsAlive);
            consumer = ConsumerFactory.create(monitoringServer);

            Runtime
                .getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> {
                            consumer.dispose();
                            monitoringServer.close();
                            latch.countDown();
                        }
                    )
                );

            monitoringServer.start();
            Monitor.started();
            latch.await();
            Monitor.serviceTerminated();
        } catch (Exception e) {}
    }
}
