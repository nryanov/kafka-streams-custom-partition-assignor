package jpoint2024;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import jpoint2024.model.TopicOneModel;
import jpoint2024.model.TopicThreeModel;
import jpoint2024.model.TopicTwoModel;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class KafkaStreamsJoiner {
    private final AtomicReference<ReadOnlyKeyValueStore<TopicOneModel.Key, TopicOneModel.Value>> topicOneStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<TopicTwoModel.Key, TopicTwoModel.Value>> topicTwoStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<TopicThreeModel.Key, TopicThreeModel.Value>> topicThreeStoreRef;

    public KafkaStreamsJoiner(
            @Named("topic-one-store")
            AtomicReference<ReadOnlyKeyValueStore<TopicOneModel.Key, TopicOneModel.Value>> topicOneStoreRef,
            @Named("topic-two-store")
            AtomicReference<ReadOnlyKeyValueStore<TopicTwoModel.Key, TopicTwoModel.Value>> topicTwoStoreRef,
            @Named("topic-three-store")
            AtomicReference<ReadOnlyKeyValueStore<TopicThreeModel.Key, TopicThreeModel.Value>> topicThreeStoreRef) {
        this.topicOneStoreRef = topicOneStoreRef;
        this.topicTwoStoreRef = topicTwoStoreRef;
        this.topicThreeStoreRef = topicThreeStoreRef;
    }

    public void join() {
        // some join logic
    }
}
