package jpoint2024;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import jpoint2024.model.TopicOneModel;
import jpoint2024.model.TopicThreeModel;
import jpoint2024.model.TopicTwoModel;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.jboss.logging.Logger;

import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class KafkaStreamsJoiner {
    private final Logger logger;
    private final AtomicReference<ReadOnlyKeyValueStore<TopicOneModel.Key, TopicOneModel.Value>> topicOneStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<TopicTwoModel.Key, TopicTwoModel.Value>> topicTwoStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<TopicThreeModel.Key, TopicThreeModel.Value>> topicThreeStoreRef;

    public KafkaStreamsJoiner(
            Logger logger,
            @Named("topic-one-store")
            AtomicReference<ReadOnlyKeyValueStore<TopicOneModel.Key, TopicOneModel.Value>> topicOneStoreRef,
            @Named("topic-two-store")
            AtomicReference<ReadOnlyKeyValueStore<TopicTwoModel.Key, TopicTwoModel.Value>> topicTwoStoreRef,
            @Named("topic-three-store")
            AtomicReference<ReadOnlyKeyValueStore<TopicThreeModel.Key, TopicThreeModel.Value>> topicThreeStoreRef) {
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

            // process each record as you wish

        } catch (InvalidStateStoreException e) {
            logger.warnf("State is currently migrating: %s", e.getMessage());
        }
    }
}
