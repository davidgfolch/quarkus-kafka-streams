package org.acme.kafka.streams.aggregator.model;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.acme.kafka.streams.aggregator.Constants.STORE_WEATHER_STATIONS;

@RegisterForReflection
public class WeatherStationTemperature {

    public static final String TOPIC = "temperatures-aggregated";
    public static final ObjectMapperSerde<WeatherStationTemperature> SERDE = new ObjectMapperSerde<>(WeatherStationTemperature.class);
    public static final Materialized<Integer, WeatherStationTemperature, KeyValueStore<Bytes, byte[]>> MATERIALIZED =
            Materialized.<Integer, WeatherStationTemperature>as(Stores.persistentKeyValueStore(STORE_WEATHER_STATIONS))
                    .withKeySerde(Serdes.Integer()).withValueSerde(SERDE);
    public static final Produced<Integer, WeatherStationTemperature> PRODUCED = Produced.with(Serdes.Integer(), SERDE);

    public int stationId;
    public String stationName;
    public double min = Double.MAX_VALUE;
    public double max = Double.MIN_VALUE;
    public int count;
    public double sum;
    public double avg;

    public WeatherStationTemperature updateFrom(Temperature t) {
        stationId = t.stationId;
        stationName = t.stationName;
        count++;
        sum += t.value;
        avg = BigDecimal.valueOf(sum / count).setScale(1, RoundingMode.HALF_UP).doubleValue();
        min = Math.min(min, t.value);
        max = Math.max(max, t.value);
        return this;
    }
}
