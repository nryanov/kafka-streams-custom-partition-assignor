package jpoint2024;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Produces;
import javax.inject.Named;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class KafkaStreamsConfiguration {
    private final Logger logger;
    private KafkaStreams kafkaStreams;

    private final AtomicReference<ReadOnlyKeyValueStore<String, String>> topicOneStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<String, String>> topicTwoStoreRef;
    private final AtomicReference<ReadOnlyKeyValueStore<String, String>> topicThreeStoreRef;

    private final AtomicReference<KafkaStreams> assignedPartitions;

    private final String topicStoreNameOne = "topic-store-1";
    private final String topicStoreNameTwo = "topic-store-2";
    private final String topicStoreNameThree = "topic-store-3";

    public KafkaStreamsConfiguration(Logger logger) {
        this.logger = logger;

        this.topicOneStoreRef = new AtomicReference<>();
        this.topicTwoStoreRef = new AtomicReference<>();
        this.topicThreeStoreRef = new AtomicReference<>();
        this.assignedPartitions = new AtomicReference<>();

        buildTopology();
    }

    private void buildTopology() {
        var builder = new StreamsBuilder();

        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "jpoint2024");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        var topicOneKeySerde = Serdes.String();
        var topicTwoKeySerde = Serdes.String();
        var topicThreeKeySerde = Serdes.String();

        var topicOneStoreSupplier = Stores.inMemoryKeyValueStore(topicStoreNameOne);
        var topicTwoStoreSupplier = Stores.inMemoryKeyValueStore(topicStoreNameTwo);
        var topicThreeStoreSupplier = Stores.inMemoryKeyValueStore(topicStoreNameThree);

        var topicOneStoreBuilder = Stores.keyValueStoreBuilder(topicOneStoreSupplier, topicOneKeySerde, Serdes.String()).withLoggingEnabled(Map.of()).withCachingDisabled();
        var topicTwoStoreBuilder = Stores.keyValueStoreBuilder(topicTwoStoreSupplier, topicTwoKeySerde, Serdes.String()).withLoggingEnabled(Map.of()).withCachingDisabled();
        var topicThreeStoreBuilder = Stores.keyValueStoreBuilder(topicThreeStoreSupplier, topicThreeKeySerde, Serdes.String()).withLoggingEnabled(Map.of()).withCachingDisabled();

        builder.addStateStore(topicOneStoreBuilder);
        builder.addStateStore(topicTwoStoreBuilder);
        builder.addStateStore(topicThreeStoreBuilder);

        var topicOneKTable = builder.table("topic1", Consumed.with(topicOneKeySerde, Serdes.String()));
        var topicTwoKTable = builder.table("topic2", Consumed.with(topicTwoKeySerde, Serdes.String()));
        var topicThreeKTable = builder.table("topic3", Consumed.with(topicThreeKeySerde, Serdes.String()));

        topicOneKTable.transformValues(() -> createStoreProducer(topicStoreNameOne), topicStoreNameOne).toStream().foreach((key, value) -> {});
        topicTwoKTable.transformValues(() -> createStoreProducer(topicStoreNameTwo), topicStoreNameTwo).toStream().foreach((key, value) -> {});
        topicThreeKTable.transformValues(() -> createStoreProducer(topicStoreNameThree), topicStoreNameThree).toStream().foreach((key, value) -> {});

        var topology = builder.build(properties);
        kafkaStreams = new KafkaStreams(topology, new CustomStreamsConfig(properties));
//        kafkaStreams = new KafkaStreams(topology, properties);
    }

    private static <K, V> ValueTransformerWithKey<K, V, V> createStoreProducer(String storageName) {
        return new ValueTransformerWithKey<>() {
            private KeyValueStore<K, V> storage;
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.storage = (KeyValueStore<K, V>) context.getStateStore(storageName);
                this.context = context;
            }

            @Override
            public V transform(K readOnlyKey, V value) {
                storage.put(readOnlyKey, value);

                return value;
            }

            @Override
            public void close() {
            }
        };
    }

    @Produces
    @Named("topic-one-store")
    public AtomicReference<ReadOnlyKeyValueStore<String, String>> topicOneStore() {
        return topicOneStoreRef;
    }

    @Produces
    @Named("topic-two-store")
    public AtomicReference<ReadOnlyKeyValueStore<String, String>> topicTwoStore() {
        return topicTwoStoreRef;
    }

    @Produces
    @Named("topic-three-store")
    public AtomicReference<ReadOnlyKeyValueStore<String, String>> topicThreeStore() {
        return topicThreeStoreRef;
    }

    @Produces
    @Named("assigned-partitions")
    public AtomicReference<KafkaStreams> assignedPartitions() {
        return assignedPartitions;
    }

    public void onStartup(@Observes StartupEvent event) {
        logger.infof("Starting kafka streams");

        kafkaStreams.setUncaughtExceptionHandler((t, e) -> logger.errorf("Unexpected error: %s", e.getMessage()));

        kafkaStreams.setStateListener((newState, oldState) -> {
            logger.infof("Kafka streams has changed state from %s to %s", oldState.name(), newState.name());

            if (newState == KafkaStreams.State.RUNNING) {
                ReadOnlyKeyValueStore<String, String> topicOneStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(topicStoreNameOne, QueryableStoreTypes.keyValueStore()));
                ReadOnlyKeyValueStore<String, String> topicTwoStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(topicStoreNameTwo, QueryableStoreTypes.keyValueStore()));
                ReadOnlyKeyValueStore<String, String> topicThreeStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(topicStoreNameThree, QueryableStoreTypes.keyValueStore()));

                topicOneStoreRef.set(topicOneStore);
                topicTwoStoreRef.set(topicTwoStore);
                topicThreeStoreRef.set(topicThreeStore);

                assignedPartitions.set(kafkaStreams);
            } else {
                topicOneStoreRef.set(null);
                topicTwoStoreRef.set(null);
                topicThreeStoreRef.set(null);
                assignedPartitions.set(null);
            }
        });

        kafkaStreams.start();
    }

    public void onShutdown(@Observes ShutdownEvent event) {
        logger.infof("Stopping kafka streams");
        kafkaStreams.close(Duration.ofSeconds(30));
        logger.infof("Successfully stopped kafka streams");
    }
}
