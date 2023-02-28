package org.acme.kafka.streams.producer.generator;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.Record;
import org.acme.kafka.streams.aggregator.model.Temperature;
import org.acme.kafka.streams.aggregator.model.WeatherStation;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import static java.math.RoundingMode.HALF_UP;
import static java.time.Duration.ofMillis;

/**
 * A bean producing random temperature data every second.
 * The values are written to a Kafka topic (temperature-values).
 * Another topic contains the name of weather stations (weather-stations).
 * The Kafka configuration is specified in the application configuration.
 */
@ApplicationScoped
public class ValuesGenerator {

    private static final Logger LOG = Logger.getLogger(ValuesGenerator.class);

    private final Random random = new Random();
    private final List<WeatherStationTemperature> stations = List.of(
            new WeatherStationTemperature(1, "Hamburg", 13),
            new WeatherStationTemperature(2, "Snowdonia", 5),
            new WeatherStationTemperature(3, "Boston", 11),
            new WeatherStationTemperature(4, "Tokio", 16),
            new WeatherStationTemperature(5, "Cusco", 12),
            new WeatherStationTemperature(6, "Svalbard", -7),
            new WeatherStationTemperature(7, "Porthsmouth", 11),
            new WeatherStationTemperature(8, "Oslo", 7),
            new WeatherStationTemperature(9, "Marrakesh", 20));

    @Outgoing(Temperature.TOPIC)
    public Multi<Record<Integer, String>> generate() {
        return Multi.createFrom().ticks().every(ofMillis(500)).onOverflow().drop().map(tick -> {
            WeatherStationTemperature station = stations.get(random.nextInt(stations.size()));
            double temperature = BigDecimal.valueOf(random.nextGaussian() * 15 + station.max)
                    .setScale(1, HALF_UP).doubleValue();
            LOG.infov("station: {0}, temperature: {1}", station.stationName, temperature);
            return Record.of(station.stationId, Instant.now() + ";" + temperature);
        });
    }

    @Outgoing(WeatherStation.TOPIC)
    public Multi<Record<Integer, String>> weatherStations() {
        return Multi.createFrom().items(
                stations.stream().map(s -> Record.of(s.stationId, "{ \"id\" : " + s.stationId + ", \"name\" : \"" + s.stationName + "\" }")));
    }

    private static class WeatherStationTemperature extends org.acme.kafka.streams.aggregator.model.WeatherStationTemperature {
        public WeatherStationTemperature(int stationId, String stationName, int temperature) {
            this.stationId = stationId;
            this.stationName = stationName;
            this.max = temperature;
        }
    }
}
