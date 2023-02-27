package org.acme.kafka.streams.aggregator.streams;

import org.acme.kafka.streams.aggregator.Constants;
import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHelper {

    private TestHelper() { //NOSONAR
    }

    public static Properties buildProperties(Map<?, ?> properties) {
        Properties props = new Properties();
        props.putAll(properties);
        return props;
    }

    public static <T> List<ConsumerRecord<Integer, T>> poll(Consumer<Integer, T> consumer, int expectedRecordCount) {
        AtomicInteger fetched = new AtomicInteger();
        List<ConsumerRecord<Integer, T>> result = new ArrayList<>();
        while (fetched.get() < expectedRecordCount) {
            consumer.poll(Duration.ofSeconds(5)).forEach(value -> {
                result.add(value);
                fetched.getAndIncrement();
            });
        }
        return result;
    }


}
