package kafka;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ThreadFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

class KafkaSchedulers {
    static final Logger log = Loggers.getLogger(Schedulers.class);

    static void defaultUncaughtException(Thread t, Throwable e) {
        log.error(
            "KafkaScheduler worker in group " + t.getThreadGroup().getName() + " failed with an uncaught exception",
            e
        );
    }

    static Scheduler newEvent(String groupId) {
        return Schedulers.newSingle(new EventThreadFactory(groupId));
    }

    static boolean isCurrentThreadFromScheduler() {
        return Thread.currentThread() instanceof EventThreadFactory.EmitterThread;
    }

    static final class EventThreadFactory implements ThreadFactory {
        static final String PREFIX = "reactive-kafka-";
        static final AtomicLong COUNTER_REFERENCE = new AtomicLong();

        private final String groupId;

        EventThreadFactory(String groupId) {
            this.groupId = groupId;
        }

        @Override
        public final Thread newThread(Runnable runnable) {
            String newThreadName = PREFIX + groupId + "-" + COUNTER_REFERENCE.incrementAndGet();
            Thread t = new EmitterThread(runnable, newThreadName);
            t.setUncaughtExceptionHandler(KafkaSchedulers::defaultUncaughtException);
            return t;
        }

        static final class EmitterThread extends Thread {

            EmitterThread(Runnable target, String name) {
                super(target, name);
            }
        }
    }
}
