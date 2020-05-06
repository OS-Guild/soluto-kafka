package kafka;

import configuration.Config;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import monitoring.Monitor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import target.ITarget;

public class Consumer {
    private ReactiveKafkaConsumer<String, String> kafkaConsumer;
    private final ITarget target;

    Consumer(ReactiveKafkaConsumer<String, String> kafkaConsumer, ITarget target) {
        this.kafkaConsumer = kafkaConsumer;
        this.target = target;
    }

    public Flux<?> stream() {
        return kafkaConsumer
            .onBackpressureBuffer()
            .flatMapIterable(records -> records)
            .doOnRequest(kafkaConsumer::poll)
            .groupBy(x -> x.partition(), __ -> __, Config.MAX_POLL_RECORDS)
            .delayElements(Duration.ofMillis(Config.PROCESSING_DELAY))
            .flatMap(
                partition -> partition.concatMap(
                    record -> Mono
                        .fromFuture(target.call(record))
                        .doOnSuccess(
                            targetResponse -> {
                                if (targetResponse.callLatency.isPresent()) {
                                    Monitor.callTargetLatency(targetResponse.callLatency.getAsLong());
                                }
                                if (targetResponse.resultLatency.isPresent()) {
                                    Monitor.resultTargetLatency(targetResponse.resultLatency.getAsLong());
                                }
                            }
                        )
                )
            )
            .sample(Duration.ofMillis(Config.COMMIT_INTERVAL))
            .concatMap(
                __ -> {
                    kafkaConsumer.commit();
                    return Mono.empty();
                }
            )
            .onErrorContinue(a -> a instanceof CommitFailedException, (a, v) -> {});
    }
}

class Partitioner {

    Iterable<Iterable<ConsumerRecord<String, String>>> partition(Iterable<ConsumerRecord<String, String>> records) {
        return StreamSupport
            .stream(records.spliterator(), false)
            .collect(Collectors.groupingBy(ConsumerRecord::key))
            .values()
            .stream()
            .map(this::createPartition)
            .collect(Collectors.toList());
    }

    private List<ConsumerRecord<String, String>> createPartition(List<ConsumerRecord<String, String>> consumerRecords) {
        List<ConsumerRecord<String, String>> sorted = consumerRecords
            .stream()
            .sorted(Comparator.comparingLong(ConsumerRecord::offset))
            .collect(Collectors.toList());

        // return Config.DEDUP_PARTITION_BY_KEY ? Collections.singletonList(sorted.get(0)) : sorted;
        return sorted;
    }
}
