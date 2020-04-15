import configuration.*;
import java.util.concurrent.CountDownLatch;
import kafka.*;
import monitoring.*;
import target.*;

public class Main {
    static Consumer consumer;
    static MonitoringServer monitoringServer;

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
            consumer = ConsumerFactory.create(monitoringServer);

            Runtime
                .getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> {
                            consumer.stop();
                            monitoringServer.close();
                            Monitor.serviceShutdown();
                        }
                    )
                );

            monitoringServer.start();

            consumer.start();
            Monitor.started();
            new CountDownLatch(1).await();
        } catch (Exception e) {
            Monitor.unexpectedError(e);
        } finally {
            monitoringServer.close();
            consumer.stop();
            Monitor.serviceTerminated();
        }
    }
}
