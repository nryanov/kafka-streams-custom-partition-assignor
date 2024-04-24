package jpoint2024;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class KafkaStreamsJoiner {
    private final Logger logger;
    private final AtomicReference<ReadOnlyKeyValueStore<String, String>> topicOneStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<String, String>> topicTwoStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<String, String>> topicThreeStoreRef;

    public KafkaStreamsJoiner(
            Logger logger,
            @Named("topic-one-store")
            AtomicReference<ReadOnlyKeyValueStore<String, String>> topicOneStoreRef,
            @Named("topic-two-store")
            AtomicReference<ReadOnlyKeyValueStore<String, String>> topicTwoStoreRef,
            @Named("topic-three-store")
            AtomicReference<ReadOnlyKeyValueStore<String, String>> topicThreeStoreRef) {
        this.logger = logger;
        this.topicOneStoreRef = topicOneStoreRef;
        this.topicTwoStoreRef = topicTwoStoreRef;
        this.topicThreeStoreRef = topicThreeStoreRef;
    }

    public void join() {
        var stateOne = topicOneStoreRef.get();
        var stateTwo = topicTwoStoreRef.get();
        var stateThree = topicThreeStoreRef.get();

        if (stateOne == null || stateTwo == null || stateThree == null) {
            logger.warnf("State is not ready yet");
            return;
        }

        try (
                var stateOneIterator = stateOne.all();
                var stateTwoIterator = stateTwo.all();
                var stateThreeIterator = stateThree.all()
        ) {
            var mapOne = new HashMap<String, String>();
            var mapTwo = new HashMap<String, String>();
            var mapThree = new HashMap<String, String>();

            stateOneIterator.forEachRemaining(record -> mapOne.put(record.key, record.value));
            stateTwoIterator.forEachRemaining(record -> mapTwo.put(record.key, record.value));
            stateThreeIterator.forEachRemaining(record -> mapThree.put(record.key, record.value));

            logger.infof("Current state for topic1: %s\nCurrent state for topic2: %s\nCurrent state for topic3: %s", mapOne, mapTwo, mapThree);

        } catch (InvalidStateStoreException e) {
            logger.warnf("State is currently migrating: %s", e.getMessage());
        }
    }
}
