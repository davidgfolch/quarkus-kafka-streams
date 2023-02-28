package org.acme.kafka.streams.aggregator.streams;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;
import io.quarkus.test.junit.QuarkusTest;
import org.acme.kafka.streams.aggregator.model.WeatherStationTemperature;
import org.acme.kafka.streams.aggregator.model.Temperature;
import org.acme.kafka.streams.aggregator.model.WeatherStation;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.acme.kafka.streams.aggregator.Constants.STORE_WEATHER_STATIONS;
import static org.acme.kafka.streams.aggregator.model.WeatherStationTemperature.TOPIC;
import static org.acme.kafka.streams.aggregator.streams.Constants.STATION;
import static org.acme.kafka.streams.aggregator.streams.TestHelper.buildProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Testing of the Topology without a broker, using TopologyTestDriver
 */
@QuarkusTest
public class TopologyProducerTest {

    private final Properties config = buildProperties(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "testApplicationId",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"));

    @Inject
    Topology topology;
    TopologyTestDriver testDriver;
    TestInputTopic<Integer, String> temperatures;
    TestInputTopic<Integer, WeatherStation> weatherStations;
    TestOutputTopic<Integer, WeatherStationTemperature> temperaturesAggregated;

    @BeforeEach
    public void setUp() {
        testDriver = new TopologyTestDriver(topology, config);
        temperatures = testDriver.createInputTopic(Temperature.TOPIC, new IntegerSerializer(), new StringSerializer());
        weatherStations = testDriver.createInputTopic(WeatherStation.TOPIC, new IntegerSerializer(), new ObjectMapperSerializer<>());
        temperaturesAggregated = testDriver.createOutputTopic(TOPIC, new IntegerDeserializer(), new ObjectMapperDeserializer<>(WeatherStationTemperature.class));
        testDriver.getTimestampedKeyValueStore(STORE_WEATHER_STATIONS).flush();
    }

    @AfterEach
    public void tearDown() {
        testDriver.getTimestampedKeyValueStore(STORE_WEATHER_STATIONS).flush();
        testDriver.close();
    }

    @Test
    public void test() {
        weatherStations.pipeInput(STATION.id, STATION);
        createTemperature("15");
        createTemperature("25");
        temperaturesAggregated.readRecord();
        WeatherStationTemperature result = temperaturesAggregated.readRecord().getValue();
        assertEquals(2, result.count);
        assertEquals(1, result.stationId);
        assertEquals("Station 1", result.stationName);
        assertEquals(20, result.avg);
    }

    private void createTemperature(String temperature) {
        temperatures.pipeInput(STATION.id, Instant.now() + Temperature.SEP + temperature);
    }
}
