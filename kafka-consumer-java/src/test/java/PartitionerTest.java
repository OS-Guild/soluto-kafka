import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class PartitionerTest {
    private final Partitioner partitioner;

    PartitionerTest() {
        this.partitioner = new Partitioner();
    }

    private ConsumerRecord[][] arrify(Iterable<Iterable<ConsumerRecord<String, String>>> partitions) {
        return StreamSupport
            .stream(partitions.spliterator(), false)
            .map(
                consumerRecords -> StreamSupport
                    .stream(consumerRecords.spliterator(), false)
                    .toArray(ConsumerRecord[]::new)
            )
            .toArray(ConsumerRecord[][]::new);
    }

    private List<ConsumerRecord<String, String>> shuffle(List<ConsumerRecord<String, String>> records) {
        Collections.shuffle(records);

        return records;
    }

    private List<ConsumerRecord<String, String>> createRecords(int experts, int recordsPerExpert) {
        return StreamSupport
            .stream(IntStream.range(0, experts).spliterator(), false)
            .flatMap(
                expert -> StreamSupport
                    .stream(
                        IntStream
                            .range(0, recordsPerExpert)
                            .map(value -> value + expert * recordsPerExpert)
                            .spliterator(),
                        false
                    )
                    .map(offset -> new ConsumerRecord<>("test", 0, offset, expert.toString(), ""))
            )
            .collect(Collectors.toList());
    }

    @Test
    void onePartition() {
        var partitions = partitioner.partition(createRecords(1, 1));

        var partitionsArray = arrify(partitions);

        assertEquals(1, partitionsArray.length);

        var partition = partitionsArray[0];

        assertEquals(1, partition.length);

        var record = partition[0];

        assertEquals(record.offset(), 0);
    }

    @Test
    void multiplePartitions() {
        var records = shuffle(createRecords(5, 20));

        var partitions = partitioner.partition(records);

        var partitionsArray = arrify(partitions);

        assertEquals(5, partitionsArray.length);

        for (var partition : partitionsArray) {
            assertEquals(20, partition.length);

            assertTrue(
                Arrays.stream(partition).allMatch(consumerRecord -> consumerRecord.key().equals(partition[0].key()))
            );
            for (int i = 0; i < partition.length - 1; i++) {
                assertTrue(partition[i].offset() < partition[i + 1].offset());
            }
        }
    }
}
