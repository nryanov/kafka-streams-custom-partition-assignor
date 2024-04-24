package jpoint2024;

import io.quarkus.scheduler.Scheduled;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class KafkaAssignedPartitionsLogger {
    private final Logger logger;
    private final AtomicReference<KafkaStreams> atomicReference;

    public KafkaAssignedPartitionsLogger(
            Logger logger,
            @Named("assigned-partitions") AtomicReference<KafkaStreams> atomicReference) {
        this.logger = logger;
        this.atomicReference = atomicReference;
    }

    @Scheduled(every = "10s")
    public void logAssignedPartitions() {
        var kafkaStreamsInstance = atomicReference.get();

        if (kafkaStreamsInstance == null) {
            return;
        }

        var assignedPartitions = new HashSet<TopicPartition>();

        var metadata = kafkaStreamsInstance.localThreadsMetadata();
        metadata.forEach(meta -> meta.activeTasks().forEach(task -> assignedPartitions.addAll(task.topicPartitions())));

        logger.infof("Assigned partitions: %s", assignedPartitions);
    }
}
