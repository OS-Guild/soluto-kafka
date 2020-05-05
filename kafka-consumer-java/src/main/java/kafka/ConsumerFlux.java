package kafka;

import configuration.Config;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;

public class ConsumerFlux<K, V> extends Flux<ConsumerRecords<K, V>> implements Disposable {
    final AtomicBoolean isActive = new AtomicBoolean();

    final AtomicBoolean isClosed = new AtomicBoolean();

    final PollEvent pollEvent;

    CommitEvent commitEvent;

    final Scheduler eventScheduler;

    final KafkaCreator consumerFactory;

    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    org.apache.kafka.clients.consumer.Consumer<K, V> consumerProxy;

    CoreSubscriber<? super ConsumerRecords<K, V>> actual;

    final Collection<String> topics;

    ConsumerRebalanceListener consumerRebalanceListener;

    public ConsumerFlux(
        KafkaCreator consumerFactory,
        Collection<String> topics,
        ConsumerRebalanceListener consumerRebalanceListener2
    ) {
        this.topics = topics;
        this.consumerFactory = consumerFactory;
        this.consumerRebalanceListener = consumerRebalanceListener2;
        pollEvent = new PollEvent();
        commitEvent = new CommitEvent();
        eventScheduler = KafkaSchedulers.newEvent(Config.GROUP_ID);
    }

    @Override
    public void subscribe(CoreSubscriber<? super ConsumerRecords<K, V>> actual) {
        if (!isActive.compareAndSet(false, true)) {
            Operators.error(
                actual,
                new IllegalStateException("Multiple subscribers are not supported for KafkaReceiver flux")
            );
            return;
        }

        this.actual = actual;

        isClosed.set(false);

        try {
            consumer = consumerFactory.createConsumer();
            eventScheduler.schedule(new SubscribeEvent());

            actual.onSubscribe(
                new Subscription() {

                    @Override
                    public void request(long n) {
                        if (pollEvent.requestsPending.get() > 0) {
                            System.out.println("pollEvent - initial");
                            pollEvent.scheduleIfRequired();
                        }
                    }

                    @Override
                    public void cancel() {}
                }
            );

            eventScheduler.start();
        } catch (Exception e) {
            Operators.error(actual, e);
            return;
        }
    }

    Mono<Void> commit() {
        commitEvent.scheduleIfRequired();
        return Mono.empty();
    }

    @Override
    public void dispose() {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        boolean isConsumerClosed = consumer == null;
        if (isConsumerClosed) {
            return;
        }

        try {
            consumer.wakeup();
            CloseEvent closeEvent = new CloseEvent(Duration.ofNanos(Long.MAX_VALUE));

            boolean isEventsThread = KafkaSchedulers.isCurrentThreadFromScheduler();
            if (isEventsThread) {
                closeEvent.run();
                return;
            }

            if (eventScheduler.isDisposed()) {
                closeEvent.run();
                return;
            }

            eventScheduler.schedule(closeEvent);
            isConsumerClosed = closeEvent.await();
        } finally {
            eventScheduler.dispose();

            // If the consumer was not closed within the specified timeout
            // try to close again. This is not safe, so ignore exceptions and
            // retry.
            int maxRetries = 10;
            for (int i = 0; i < maxRetries && !isConsumerClosed; i++) {
                try {
                    if (consumer != null) {
                        consumer.close();
                    }
                    isConsumerClosed = true;
                } catch (Exception e) {}
            }
            isClosed.set(true);
        }
    }

    void handleRequest(Long toAdd) {
        System.out.println("handleReqest started " + toAdd);
        var x = OperatorUtils.safeAddAndGet(pollEvent.requestsPending, toAdd);
        System.out.println("pollEvent.requestsPending " + x);

        if (x > 0) {
            System.out.println(("poll after handleRequest " + toAdd));
            pollEvent.scheduleIfRequired();
        }
    }

    class SubscribeEvent implements Runnable {

        @Override
        public void run() {
            try {
                consumer.subscribe(topics, consumerRebalanceListener);
                System.out.println("consumer.subscribe");
            } catch (Exception e) {
                if (isActive.get()) {
                    actual.onError(e);
                }
            }
        }
    }

    class CommitEvent implements Runnable {
        private final AtomicBoolean isPending = new AtomicBoolean();
        private final AtomicInteger inProgress = new AtomicInteger();

        @Override
        public void run() {
            if (!isPending.compareAndSet(true, false)) {
                return;
            }
            try {
                inProgress.incrementAndGet();
                System.out.println(("commitSync started"));
                consumer.commitSync();
                inProgress.decrementAndGet();
            // System.out.println("pollEvent after commit");
            // pollEvent.scheduleIfRequired();
            } catch (Exception e) {
                inProgress.decrementAndGet();
                actual.onError(e);
            }
        }

        void runIfRequired(boolean force) {
            if (force) isPending.set(true);
            if (isPending.get()) run();
        }

        void scheduleIfRequired() {
            if (isActive.get() && isPending.compareAndSet(false, true)) {
                eventScheduler.schedule(this);
            }
        }

        private void waitFor(long endTimeNanos) {
            while (inProgress.get() > 0 && endTimeNanos - System.nanoTime() > 0) {
                consumer.poll(Duration.ofMillis(1));
            }
        }
    }

    class PollEvent implements Runnable {
        private final AtomicInteger pendingCount = new AtomicInteger();
        private final Duration pollTimeout = Duration.ofMillis(Config.POLL_TIMEOUT);

        private final AtomicBoolean partitionsPaused = new AtomicBoolean();
        final AtomicLong requestsPending = new AtomicLong();

        @Override
        public void run() {
            try {
                if (isActive.get()) {
                    commitEvent.runIfRequired(false);
                    pendingCount.decrementAndGet();

                    // if (requestsPending.get() > 0) {
                    //     if (partitionsPaused.getAndSet(false)) {
                    //         consumer.resume(consumer.assignment());
                    //     }
                    // } else {
                    //     if (!partitionsPaused.getAndSet(true)) {
                    //         consumer.pause(consumer.assignment());
                    //     }
                    // }
                    ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    System.out.println("consumer.poll records is " + records.count());

                    if (isActive.get()) {
                        int count = records.count();
                        if (requestsPending.addAndGet(0 - count) > 0) {
                            System.out.println("pollEvent:: there are still requestsPending " + requestsPending.get());
                            scheduleIfRequired();
                        }
                    }
                    if (records.count() > 0) {
                        actual.onNext(records);
                    }
                }
            } catch (Exception e) {
                if (isActive.get()) {
                    actual.onError(e);
                }
            }
        }

        void scheduleIfRequired() {
            if (pendingCount.get() <= 0) {
                eventScheduler.schedule(this);
                pendingCount.incrementAndGet();
            }
        }
    }

    class CloseEvent implements Runnable {
        private final long closeEndTimeNanos;
        private final CountDownLatch latch = new CountDownLatch(1);

        CloseEvent(Duration timeout) {
            this.closeEndTimeNanos = System.nanoTime() + timeout.toNanos();
        }

        @Override
        public void run() {
            try {
                if (consumer != null) {
                    int attempts = 3;
                    for (int i = 0; i < attempts; i++) {
                        try {
                            commitEvent.runIfRequired(true);
                            commitEvent.waitFor(closeEndTimeNanos);
                            long timeoutNanos = closeEndTimeNanos - System.nanoTime();
                            if (timeoutNanos < 0) timeoutNanos = 0;
                            System.out.println("closing consumer!!!!!!");
                            consumer.close(Duration.ofNanos(timeoutNanos));
                            break;
                        } catch (WakeupException e) {
                            if (i == attempts - 1) throw e;
                        }
                    }
                }
                latch.countDown();
            } catch (Exception e) {
                actual.onError(e);
            }
        }

        boolean await(long timeoutNanos) throws InterruptedException {
            return latch.await(timeoutNanos, TimeUnit.NANOSECONDS);
        }

        boolean await() {
            boolean closed = false;
            long remainingNanos;
            while (!closed && (remainingNanos = closeEndTimeNanos - System.nanoTime()) > 0) {
                try {
                    closed = await(remainingNanos);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            return closed;
        }
    }
}
