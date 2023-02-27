package org.acme.kafka.streams.aggregator.streams;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.acme.kafka.streams.aggregator.model.Temperature;
import org.acme.kafka.streams.aggregator.model.WeatherStation;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.acme.kafka.streams.aggregator.streams.TestHelper.buildProperties;
import static org.acme.kafka.streams.aggregator.streams.TestHelper.poll;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration testing of the application with an embedded broker.
 */
@QuarkusTest
@QuarkusTestResource(KafkaResource.class)
public class AggregatorTest {

    KafkaProducer<Integer, String> temperatureProducer;
    KafkaProducer<Integer, WeatherStation> stationsProducer;
    KafkaConsumer<Integer, Aggregation> aggConsumer;

    @BeforeEach
    public void setUp() {
        Properties producerCfg = buildProperties(Map.of(BOOTSTRAP_SERVERS_CONFIG, KafkaResource.getBootstrapServers()));
        temperatureProducer = new KafkaProducer<>(producerCfg, new IntegerSerializer(), new StringSerializer());
        stationsProducer = new KafkaProducer<>(producerCfg, new IntegerSerializer(), new ObjectMapperSerializer<>());
        final Properties consumerCfg = buildProperties(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, KafkaResource.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-group-id",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        new File("/tmp/kafka-streams/temperature-aggregator").delete(); //native exec
        new File("target/data/kafka-data/stores/temperature-aggregator").delete(); //non native exec
        aggConsumer = new KafkaConsumer<>(consumerCfg, new IntegerDeserializer(), new ObjectMapperDeserializer<>(Aggregation.class));
    }

    @AfterEach
    public void tearDown() {
        temperatureProducer.close();
        stationsProducer.close();
        aggConsumer.close();
    }

    @Test
    @Timeout(value = 30)
    public void test() {
        aggConsumer.subscribe(List.of(Aggregation.TOPIC));
        stationsProducer.send(new ProducerRecord<>(WeatherStation.TOPIC, 1, new WeatherStation(1, "Station 1")));
        temperatureProducer.send(new ProducerRecord<>(Temperature.TOPIC, 1, Instant.now() + ";" + "15"));
        temperatureProducer.send(new ProducerRecord<>(Temperature.TOPIC, 1, Instant.now() + ";" + "25"));
        Aggregation results = poll(aggConsumer, 1).get(0).value();
        assertEquals(2, results.count); // When the state store was initially empty is 2 and so on (+2)
        assertEquals(1, results.stationId);
        assertEquals("Station 1", results.stationName);
        assertEquals(20, results.avg);
    }

}
