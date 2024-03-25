package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.processor.internals.CustomStreamPartitionAssignor;

import java.util.Map;

public class CustomStreamsConfig extends StreamsConfig {
    public CustomStreamsConfig(Map<?, ?> props) {
        super(props);
    }

    @Override
    public Map<String, Object> getMainConsumerConfigs(String groupId, String clientId, int threadIdx) {
        var configs = super.getMainConsumerConfigs(groupId, clientId, threadIdx);

        configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CustomStreamPartitionAssignor.class.getName());

        return configs;
    }
}
