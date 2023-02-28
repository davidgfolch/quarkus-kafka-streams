package org.acme.kafka.streams.aggregator.streams;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

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

    @NotNull
    public static <T> KafkaConsumer<Integer, T> newConsumerIntOm(Map<String, String> consumerCfg, Class<T> clazz) {
        return new KafkaConsumer<>(buildProperties(consumerCfg), new IntegerDeserializer(), new ObjectMapperDeserializer<>(clazz));
    }

    @NotNull
    public static <T> KafkaProducer<Integer, T> newProducerIntOm(Map<String, String> prodCfg) {
        return new KafkaProducer<>(buildProperties(prodCfg), new IntegerSerializer(), new ObjectMapperSerializer<>());
    }

    @NotNull
    public static KafkaProducer<Integer, String> newProducerIntString(Map<String, String> prodCfg) {
        return new KafkaProducer<>(buildProperties(prodCfg), new IntegerSerializer(), new StringSerializer());
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
